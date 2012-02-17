package org.apache.hadoop.fs.swift;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.HashMap;
import java.util.List;

import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesContainerExistsException;
import com.rackspacecloud.client.cloudfiles.FilesContainerInfo;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

public class FilesClientWrapper implements ISwiftFilesClient {

	private FilesClient client;

	public FilesClientWrapper(FilesClient client) {
		this.client = client;
		try {
			this.client.login();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public InputStream getObjectAsStream(String container, String objName,
			long pos) {
		try {
			return client.getObjectAsStream(container, objName, pos, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void storeStreamedObject(String container,
			PipedInputStream fromPipe, String string, String objName,
			HashMap<String, String> hashMap) {
		try {
			client.storeStreamedObject(container, fromPipe, string, objName, hashMap);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean deleteContainer(String container) {
		try {
			return client.deleteContainer(container);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean deleteObject(String container, String object) {
		try {
			client.deleteObject(container, object);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public FilesObjectMetaData getObjectMetaData(String container,
			String objName) {
		try {
			return client.getObjectMetaData(container, objName);
		} catch (FilesNotFoundException fe) {
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FilesContainer> listContainers() {
		try {
			return client.listContainers();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FilesObject> listObjectsStartingWith(String container,
			String objName, int i, Character character) {
		try {
			return client.listObjectsStartingWith(container, objName, null, i, null, character);
		} catch (FilesNotFoundException fe) {
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean createContainer(String container) {		
		try {
			client.createContainer(container);
			return true;
		} catch (FilesContainerExistsException ce) {
			return false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public boolean createFullPath(String container, String object) {
		try {
			client.createFullPath(container, object);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public InputStream getObjectAsStream(String container, String objName) {
		try {
			return client.getObjectAsStream(container, objName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public List<FilesObject> listObjects(String name) {
		try {
			return client.listObjects(name);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void copyObject(String name, String name2, String container,
			String string) {
		try {
			client.copyObject(name, name2, container, string);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public FilesContainerInfo getContainerInfo (String container)
	{		
		try {
			return client.getContainerInfo(container);
					
		}catch(FilesNotFoundException e)
		{
			return null;
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("getContainerInfo failed", e);
		}
		
	}
}
