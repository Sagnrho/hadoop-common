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

import java.net.URI;

import org.apache.hadoop.fs.Path;

/**
 * @author Constantine Peresypkin
 *
 * <p>
 * Maps from {@link Path} to Swift container-object hierarchy
 * </p>
 * 
 */
public class SwiftPath extends Path {

	private String objName;
	private String container;

	public SwiftPath(URI aUri) {
		super(aUri);
		String fullPath = aUri.getPath().substring(1);
		int firstSlash = fullPath.indexOf('/');
		if (firstSlash > 0) {
			this.container = fullPath.substring(0, firstSlash);
			this.objName = fullPath.substring(firstSlash + 1);
		} else {
			this.container = fullPath;
			this.objName = null;
		}
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
