package org.apache.hadoop.fs.swift;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpException;

import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesContainer;
import com.rackspacecloud.client.cloudfiles.FilesContainerNotEmptyException;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObject;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

public interface ISwiftFilesClient {

	InputStream getObjectAsStream(String container, String objName, long pos,
			Object object) throws IOException, HttpException,
			FilesAuthorizationException, FilesInvalidNameException,
			FilesNotFoundException;

	void storeStreamedObject(String container, PipedInputStream fromPipe,
			String string, String objName, HashMap<String, String> hashMap)
			throws IOException, HttpException, FilesException;

	boolean deleteContainer(String container) throws IOException,
			HttpException, FilesAuthorizationException,
			FilesInvalidNameException, FilesNotFoundException,
			FilesContainerNotEmptyException;

	void deleteObject(String container, String object) throws IOException,
			FilesNotFoundException, HttpException, FilesException;

	FilesObjectMetaData getObjectMetaData(String container, String objName)
			throws IOException, FilesNotFoundException, HttpException,
			FilesAuthorizationException, FilesInvalidNameException;

	List<FilesContainer> listContainers() throws IOException, HttpException,
			FilesAuthorizationException, FilesException;

	List<FilesObject> listObjectsStartingWith(String container, String objName,
			Object object, int i, Object object2, Character character)
			throws IOException, FilesException;

	void createContainer(String container) throws IOException, HttpException,
			FilesAuthorizationException, FilesException;

	void createFullPath(String container, String object) throws HttpException,
			IOException, FilesException;

	InputStream getObjectAsStream(String container, String objName)
			throws IOException, HttpException, FilesAuthorizationException,
			FilesInvalidNameException, FilesNotFoundException;

	List<FilesObject> listObjects(String name) throws IOException,
			FilesAuthorizationException, FilesException;

	void copyObject(String name, String name2, String container, String string)
			throws HttpException, IOException;

}
