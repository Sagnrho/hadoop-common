/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpException;

import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

/**
 * @author Constantine Peresypkin
 *
 * <p>
 * A distributed implementation of {@link FileSystem} 
 * for reading and writing files on 
 * <a href="http://swift.openstack.org/">Openstack Swift</a>.
 * </p>
 */
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

	private FilesClient client;
	private Path workingDir;
	private URI uri;

	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		if (client == null) {
			client = createDefaultClient(uri, conf);
		}
		setConf(conf);
		this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
		this.workingDir =
				new Path("/user", System.getProperty("user.name")).makeQualified(this);
	}

	private static FilesClient createDefaultClient(URI uri, Configuration conf) {
		
		FilesClient client = createSwiftClient(uri, conf);

		RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
				conf.getInt("fs.swift.maxRetries", 4),
				conf.getLong("fs.swift.sleepTimeSeconds", 10), TimeUnit.SECONDS);
		Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
				new HashMap<Class<? extends Exception>, RetryPolicy>();
		exceptionToPolicyMap.put(IOException.class, basePolicy);
		exceptionToPolicyMap.put(S3Exception.class, basePolicy);

		RetryPolicy methodPolicy = RetryPolicies.retryByException(
				RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
		Map<String, RetryPolicy> methodNameToPolicyMap =
				new HashMap<String, RetryPolicy>();
		methodNameToPolicyMap.put("storeStreamedObject", methodPolicy);

		return (FilesClient)
				RetryProxy.create(FilesClient.class, client,
						methodNameToPolicyMap);
	}
	
	private static FilesClient createSwiftClient(URI uri, Configuration conf) {
		String userName = null;
		String userSecret = null;
		String authUrl = null;
		String account = null;
		String connectionTimeOut = null;

		if (uri.getHost() == null) {
			throw new IllegalArgumentException("Invalid hostname in URI " + uri);
		}

		String userInfo = uri.getUserInfo();
		if (userInfo != null) {
			int index = userInfo.indexOf(':');
			if (index != -1) {
				userName = userInfo.substring(0, index);
				userSecret = userInfo.substring(index + 1);
			} else {
				userName = userInfo;
			}
		}

		String scheme = uri.getScheme();
		String userNameProperty = String.format("fs.%s.swiftUserName", scheme);
		String userSecretProperty = String.format("fs.%s.swiftUserPassword", scheme);
		if (userName == null) {
			userName = conf.get(userNameProperty);
		}
		if (userSecret == null) {
			userSecret = conf.get(userSecretProperty);
		}
		if (userName == null && userSecret == null) {
			throw new IllegalArgumentException("Swift " +
					"User Name and Password " +
					"must be specified as the " +
					"username or password " +
					"(respectively) of a " + scheme +
					" URL, or by setting the " +
					userNameProperty + " or " +
					userSecretProperty +
					" properties (respectively).");
		} else if (userName == null) {
			throw new IllegalArgumentException("Swift " +
					"User Name must be specified " +
					"as the username of a " + scheme +
					" URL, or by setting the " +
					userNameProperty + " property.");
		} else if (userSecret == null) {
			throw new IllegalArgumentException("Swift " +
					"User Password must be " +
					"specified as the password of a " +
					scheme + " URL, or by setting the " +
					userSecretProperty +
					" property.");       
		}
		String authUrlProperty = String.format("fs.%s.swiftAuthUrl", scheme);
		String accountNameProperty = String.format("fs.%s.swiftAccountName", scheme);
		if (authUrl == null) {
			authUrl = conf.get(authUrlProperty);
		}
		if (account == null) {
			account = conf.get(accountNameProperty);
		}
		if (authUrl == null) {
			throw new IllegalArgumentException(
					"Swift Auth Url must be specified by setting the " +
							authUrlProperty +
					" property.");
		}
		if (account == null) {
			throw new IllegalArgumentException(
					"Swift Account Name must be specified by setting the " +
							accountNameProperty +
					" property.");
		}
		String timeoutProperty = String.format("fs.%s.swiftTimeout", scheme);
		if (connectionTimeOut == null) {
			connectionTimeOut = conf.get(timeoutProperty);
		}
		if (connectionTimeOut == null) {
			throw new IllegalArgumentException(
					"Swift Connection Timeout (in ms) " +
					"must be specified by setting the " +
							timeoutProperty +
					" property (0 means indefinite timeout).");
		}
		return new FilesClient(userName, userSecret, authUrl, account, Integer.parseInt(connectionTimeOut));
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		SwiftPath absolutePath = makeAbsolute(f);
		if (exists(f) && !overwrite) {
			throw new IOException("create: "+ absolutePath +": File already exists");
		}

		if (absolutePath.isContainer() || getFileStatus(f).isDir()) {
			throw new IOException("create: "+ absolutePath +": Is a directory");
		}
		
		return new FSDataOutputStream(
				new SwiftFsOutputStream(client, absolutePath.getContainer(), 
						absolutePath.getObject(), bufferSize, progress), 
						statistics);
	}

	@Override
	@Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	@Override
	public boolean delete(Path f, boolean recursive) throws IOException {
		FileStatus status;
		try {
			status = getFileStatus(f);
		} catch (FileNotFoundException e) {
			return false;
		}
		SwiftPath absolutePath = makeAbsolute(f);
		if (status.isDir()) {
			FileStatus[] contents = listStatus(f);
			if (!recursive && contents.length > 0) {
				throw new IOException("delete: " + f + ": Directory is not empty");
			}
			for (FileStatus p : contents) {
				return delete(p.getPath(), recursive);
			}
		}
		try {
			if (absolutePath.isContainer()) {
				return client.deleteContainer(absolutePath.getContainer());
			}
			client.deleteObject(absolutePath.getContainer(), absolutePath.getObject());
			return true;
		} catch (FilesNotFoundException e) {
			e.printStackTrace();
		} catch (FilesException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		SwiftPath absolutePath = makeAbsolute(f);

		String container = absolutePath.getContainer();
		if (container.length() == 0) { // root always exists
			return newDirectory(absolutePath);
		}

		if (absolutePath.isContainer()) { // container is a "directory"
			return newDirectory(absolutePath);
		}
		
		String objName = absolutePath.getObject();
		
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

	private SwiftPath makeAbsolute(Path path) {
		if (path.isAbsolute()) {
			return new SwiftPath(path.toUri());
		}
		return new SwiftPath((new Path(workingDir, path)).toUri());
	}

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
		FileStatus stat = getFileStatus(f);
		if (stat != null && ! stat.isDir()) {
			return new FileStatus[] { stat }; 
		}
		SwiftPath absolutePath = makeAbsolute(f);
		String container = absolutePath.getContainer();
		try {
			List<FileStatus> statList = new ArrayList<FileStatus>();
			if (container.length() == 0) { // we are listing root dir
				List<FilesContainer> containerList = client.listContainers();
				for (FilesContainer cont : containerList) {
					statList.add(newDirectory(new Path(cont.getName())));
				}
				return statList.toArray(new FileStatus[0]);
			}
			String objName = absolutePath.getObject();
			List<FilesObject> objList = client.listObjectsStartingWith(container, objName, null, -1, null, new Character('/'));
			for (FilesObject cont : objList) {
				statList.add(newDirectory(new Path(cont.getName())));
			}
			if (stat == null && statList.size() == 0) {
				throw new FileNotFoundException("list: "+ absolutePath +
						": No such file or directory");
			}
			return statList.toArray(new FileStatus[0]);
		} catch (FilesAuthorizationException e) {
			e.printStackTrace();
		} catch (FilesException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		}
		throw new FileNotFoundException("list: "+ absolutePath +
				": No such file or directory");
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		try {
			SwiftPath absolutePath = makeAbsolute(f);
			if (absolutePath.isContainer()) {
				client.createContainer(absolutePath.getContainer());
			} else {
				client.createFullPath(absolutePath.getContainer(), absolutePath.getObject());
			}
		} catch (FilesException e) {
			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		}
		return true;
	}

//	private boolean isContainer(Path f) {
//		Path absolutePath = makeAbsolute(f);
//		if (absolutePath.toUri().getPath() == null)
//			return true;
//		if (absolutePath.toUri().getPath().length() == 0)
//			return true;
//		return false;
//	}
	
	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		FileStatus stat = getFileStatus(f);
		if (stat == null) {
			throw new FileNotFoundException(f.toString());
		} 
		if (stat.isDir()) {
			throw new IOException("open: "+ f +": Is a directory");
		}
		
		SwiftPath absolutePath = makeAbsolute(f);
		String container = absolutePath.getContainer();
		String objName = absolutePath.getObject();
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
		SwiftPath srcAbsolute = makeAbsolute(src);
		SwiftPath dstAbsolute = makeAbsolute(dst);
		if (srcAbsolute.getContainer().length() == 0) {
			return false; // renaming root
		}
		try {
			if (getFileStatus(dstAbsolute).isDir()) {
				dstAbsolute = new SwiftPath((new Path (dstAbsolute, srcAbsolute.getName())).toUri());
			} else {
				return false; // overwrite existing file
			}
		} catch (FileNotFoundException e) {
			try {
				if (!getFileStatus(dstAbsolute.getParent()).isDir()) {
					return false; // parent dst is a file
				}
			} catch (FileNotFoundException ex) {
				return false; // parent dst does not exist
			}
		}
		try {
			if (getFileStatus(srcAbsolute).isDir()) {
				if (srcAbsolute.getContainer().length() == 0) {
					List<FilesContainer> fullList = client.listContainers();
					for (FilesContainer container : fullList) {
						List<FilesObject> list = client.listObjects(container.getName());
						for (FilesObject fobj : list) {
							client.copyObject(container.getName(), fobj.getName(), 
									dstAbsolute.getContainer(), dstAbsolute.getObject() + fobj.getName());
							client.deleteObject(container.getName(), fobj.getName());
						}
					}
				} else {
					List<FilesObject> list = client.listObjectsStartingWith(srcAbsolute.getContainer(), srcAbsolute.getObject(), 
						null, -1, null, null);
					for (FilesObject fobj : list) {
						client.copyObject(srcAbsolute.getContainer(), fobj.getName(), 
								dstAbsolute.getContainer(), dstAbsolute.getObject() + fobj.getName());
						client.deleteObject(srcAbsolute.getContainer(), fobj.getName());
					}
				}
			} else {
				if (dstAbsolute.getObject() == null)
					return false; // tried to rename object to container
				client.copyObject(srcAbsolute.getContainer(), srcAbsolute.getObject(), 
						dstAbsolute.getContainer(), dstAbsolute.getObject());
				client.deleteObject(srcAbsolute.getContainer(), srcAbsolute.getObject());
			}
			createParent(src);
			return true;
		} catch (FileNotFoundException e) {
			// Source file does not exist;
			return false;
		} catch (HttpException e) {
			e.printStackTrace();
		}
		return false;
	}

	private void createParent(Path path) throws IOException {
		Path parent = path.getParent();
		if (parent != null) {
			SwiftPath absolutePath = makeAbsolute(parent);
			if (absolutePath.getContainer().length() > 0) {
				mkdirs(absolutePath);
			}
		}
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

