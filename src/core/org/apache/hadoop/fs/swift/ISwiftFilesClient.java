package org.apache.hadoop.fs.swift;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.HashMap;
import java.util.List;

import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

public interface ISwiftFilesClient {

	InputStream getObjectAsStream(String container, String objName, long pos);

	void storeStreamedObject(String container, PipedInputStream fromPipe,
			String string, String objName, HashMap<String, String> hashMap);

	boolean deleteContainer(String container);

	boolean deleteObject(String container, String object);

	FilesObjectMetaData getObjectMetaData(String container, String objName);

	List<FilesContainer> listContainers();

	List<FilesObject> listObjectsStartingWith(String container, String objName,
			int i, Character character);

	boolean createContainer(String container);

	boolean createFullPath(String container, String object);

	InputStream getObjectAsStream(String container, String objName);

	List<FilesObject> listObjects(String name);

	void copyObject(String name, String name2, String container, String string);

}
