package org.apache.hadoop.fs.swift;

import java.net.URI;

import org.apache.hadoop.fs.Path;

public class SwiftPath extends Path {

	private String objName;
	private String container;

	public SwiftPath(URI aUri) {
		super(aUri);
		this.container = aUri.getHost();
		if (aUri.getPath() != null)
			this.objName = aUri.getPath().substring(1);
	}

	public String getContainer() {
		return container;
	}
	
	public String getObject() {
		return objName;
	}

	public boolean isContainer() {
		if (objName == null)
			return true;
		return false;
	}
}
