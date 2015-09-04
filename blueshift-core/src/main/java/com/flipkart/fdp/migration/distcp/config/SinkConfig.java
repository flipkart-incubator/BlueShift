/*
 *
 *  Copyright 2015 Flipkart Internet Pvt. Ltd.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.flipkart.fdp.migration.distcp.config;

import java.util.List;

import com.google.gson.Gson;

public class SinkConfig implements ConnectableConfig {

	private String path = null;

	private String customSinkImplClass = null;

	private String compressionCodec = "snappy";

	private boolean useCompression = false;

	private boolean overwriteFiles = false;

	private boolean append = false;

	private List<ConnectionConfig> connectionConfig = null;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getCustomSinkImplClass() {
		return customSinkImplClass;
	}

	public ConnectionConfig getDefaultConnectionConfig() {
		return connectionConfig.get(0);
	}

	public List<ConnectionConfig> getConnectionConfig() {
		return connectionConfig;
	}

	public void setConnectionConfig(List<ConnectionConfig> connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	public void setCustomSinkImplClass(String customSinkImplClass) {
		this.customSinkImplClass = customSinkImplClass;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	public boolean isUseCompression() {
		return useCompression;
	}

	public void setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
	}

	public boolean isOverwriteFiles() {
		return overwriteFiles;
	}

	public void setOverwriteFiles(boolean overwriteFiles) {
		this.overwriteFiles = overwriteFiles;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
