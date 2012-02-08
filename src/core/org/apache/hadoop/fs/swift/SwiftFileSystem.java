package org.apache.hadoop.fs.swift;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.SimpleTimeZone;

import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;

import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

public class SwiftFileSystem extends FileSystem {

	private static final long MAX_SWIFT_FILE_SIZE = 5 * 1024 * 1024 * 1024L;
	private static final String FOLDER_MIME_TYPE = "application/directory";

	private class SwiftFsInputStream extends FSInputStream {

		private InputStream in;
		private long pos = 0;
		private String objName;
		private String container;

		public SwiftFsInputStream(InputStream in, String container, String objName) {
			this.in = in;
			this.objName = objName;
			this.container = container;
		}

		@Override
		public long getPos() throws IOException {
			return pos;
		}

		@Override
		public void seek(long pos) throws IOException {
			try {
				in.close();
				in = client.getObjectAsStream(container, objName, pos, null);
				this.pos = pos;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public boolean seekToNewSource(long targetPos) throws IOException {
			return false;
		}

		@Override
		public int read() throws IOException {
			int result = in.read();
			if (result != -1) {
				pos++;
			}
			return result;
		}

		public synchronized int read(byte[] b, int off, int len)
				throws IOException {

			int result = in.read(b, off, len);
			if (result > 0) {
				pos += result;
			}
			return result;
		}

		public void close() throws IOException {
			in.close();
		}
	}

	private class SwiftFsOutputStream extends OutputStream {

		private PipedOutputStream toPipe;
		private PipedInputStream fromPipe;
		private SwiftProgress callback;
		private long pos = 0;

		public SwiftFsOutputStream(final FilesClient client, final String container,
				final String objName, int bufferSize, Progressable progress) throws IOException {
			this.toPipe = new PipedOutputStream();
			this.fromPipe = new PipedInputStream(toPipe, bufferSize);
			this.callback = new SwiftProgress(progress);

			new Thread() {
				public void run(){
					try {
						client.storeStreamedObject(container, fromPipe, "binary/octet-stream", objName, new HashMap<String, String>());
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			}.start();

		}

		@Override
		public synchronized void write(int b) throws IOException {
			toPipe.write(b);
			pos++;
			callback.progress(pos);
		}

		@Override
		public synchronized void write(byte[] b) throws IOException {
			toPipe.write(b);
			pos += b.length;
			callback.progress(pos);
		}

		@Override
		public synchronized void write(byte[] b, int off, int len) {
			try {
				toPipe.write(b, off, len);
				pos += len;
				callback.progress(pos);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public synchronized void flush() {
			try {
				toPipe.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public synchronized void close() {
			try {
				toPipe.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	FilesClient client;
	private Path workingDir;
	private URI uri;

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		Path absolutePath = makeAbsolute(f);
		if (exists(f) && !overwrite) {
			throw new IOException("create: "+ absolutePath +": File already exists");
		}

		if (isContainer(absolutePath) || getFileStatus(f).isDir()) {
			throw new IOException("create: "+ absolutePath +": Is a directory");
		}
		
		return new FSDataOutputStream(
				new SwiftFsOutputStream(client, absolutePath.toUri().getHost(), 
						absolutePath.toUri().getPath(), bufferSize, progress), 
						statistics);
	}

	@Override
	@Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, 
			long start, long len) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		Path absolutePath = makeAbsolute(f);
		//String key = pathToKey(absolutePath);

		String container = absolutePath.toUri().getHost();
		if (container.length() == 0) { // root always exists
			return newDirectory(absolutePath);
		}

		if (isContainer(absolutePath)) { // container is a "directory"
			return newDirectory(absolutePath);
		}
		
		String objName = absolutePath.toUri().getPath();
		
		try {
			FilesObjectMetaData meta = client.getObjectMetaData(container, objName);
			if (meta != null) {
				if (FOLDER_MIME_TYPE.equals(meta.getMimeType()))
					return newDirectory(absolutePath);
				return newFile(meta, absolutePath);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		throw new FileNotFoundException("stat: "+ absolutePath +
				": No such file or directory");
	}

	private Path makeAbsolute(Path path) {
		if (path.isAbsolute()) {
			return path;
		}
		return new Path(workingDir, path);
	}

	//	private static String pathToKey(Path path) {
	//		if (!path.isAbsolute()) {
	//			throw new IllegalArgumentException("Path must be absolute: " + path);
	//		}
	//		URI uri = path.toUri();
	//		return uri.getHost() + "/" + uri.getPath();
	//	}

	//	private String getContainer(String key) {
	//		int slashLocation = key.indexOf('/');
	//		if (slashLocation == -1) {
	//			return key;
	//		}
	//		return key.substring(slashLocation + 1);
	//	}

	private FileStatus newFile(FilesObjectMetaData meta, Path path) {
		try {
			Date parsedDate = parseRfc822Date(meta.getLastModified());
			long parsedLength = Long.parseLong(meta.getContentLength());
			return new FileStatus(parsedLength, false, 1, MAX_SWIFT_FILE_SIZE,
					parsedDate.getTime(), path.makeQualified(this));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private FileStatus newDirectory(Path path) {
		return new FileStatus(0, true, 1, MAX_SWIFT_FILE_SIZE, 0,
				path.makeQualified(this));
	}

	@Override
	public URI getUri() {
		return uri;
	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		try {
			Path absolutePath = makeAbsolute(f);
			if (isContainer(absolutePath)) {
				client.createContainer(absolutePath.toUri().getHost());
			} else {
				client.createFullPath(absolutePath.toUri().getHost(), absolutePath.toUri().getPath());
			}
		} catch (FilesException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		}
		return true;
	}

	private boolean isContainer(Path f) {
		Path absolutePath = makeAbsolute(f);
		if (absolutePath.toUri().getPath() == null)
			return true;
		if (absolutePath.toUri().getPath().length() == 0)
			return true;
		return false;
	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		FileStatus stat = getFileStatus(f);
		if (stat == null) {
			throw new FileNotFoundException(f.toString());
		} 
		if (stat.isDir()) {
			throw new IOException("open: "+ f +": Is a directory");
		}
		
		Path absolutePath = makeAbsolute(f);
		String container = absolutePath.toUri().getHost();
		String objName = absolutePath.toUri().getPath();
		try {
			return new FSDataInputStream(new BufferedFSInputStream(
					new SwiftFsInputStream(client.getObjectAsStream(container, objName), container, objName), bufferSize));
		} catch (FilesAuthorizationException e) {
			e.printStackTrace();
		} catch (FilesInvalidNameException e) {
			e.printStackTrace();
		} catch (FilesNotFoundException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		Path srcAbsolute = makeAbsolute(src);
		Path dstAbsolute = makeAbsolute(dst);
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setWorkingDirectory(Path newDir) {
		this.workingDir = newDir;
	}

	public static Date parseRfc822Date(String dateString) throws ParseException {
		synchronized (rfc822DateParser) {
			return rfc822DateParser.parse(dateString);
		}
	}

	protected static final SimpleDateFormat rfc822DateParser = new SimpleDateFormat(
			"EEE, dd MMM yyyy HH:mm:ss z", Locale.US);

	static {
		rfc822DateParser.setTimeZone(new SimpleTimeZone(0, "GMT"));
	}
}

