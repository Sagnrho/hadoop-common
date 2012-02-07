package org.apache.hadoop.fs.swift;

import org.apache.hadoop.util.Progressable;

import com.rackspacecloud.client.cloudfiles.IFilesTransferCallback;

public class SwiftProgress implements IFilesTransferCallback {

	Progressable hadoopCallback;
	
	public SwiftProgress(Progressable callback) {
		this.hadoopCallback = callback;
	}
	
	@Override
	public void progress(long arg0) {
		hadoopCallback.progress();
	}

}
