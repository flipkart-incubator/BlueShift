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

import com.google.gson.Gson;

public class SourceConfig {

	private String path = null;
	private String includeListFile = null;
	private String excludeListFile = null;

	private String includeRegEx = null;
	private String excludeRegEx = null;

	private String customSourceImplClass = null;

	private long startTS = 0;
	private long endTS = 0;

	private long maxFilesize = 0;
	private long minFilesize = 0;

	private boolean deleteSource = false;
	private boolean ignoreEmptyFiles = false;
	private boolean transformSource = false;
	private boolean includeUpdatedFiles = false;
	private long compressionThreshold = 0;

	private ConnectionConfig connectionConfig = null;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getIncludeListFile() {
		return includeListFile;
	}

	public void setIncludeListFile(String includeListFile) {
		this.includeListFile = includeListFile;
	}

	public String getExcludeListFile() {
		return excludeListFile;
	}

	public void setExcludeListFile(String excludeListFile) {
		this.excludeListFile = excludeListFile;
	}

	public String getIncludeRegEx() {
		return includeRegEx;
	}

	public void setIncludeRegEx(String includeRegEx) {
		this.includeRegEx = includeRegEx;
	}

	public String getExcludeRegEx() {
		return excludeRegEx;
	}

	public void setExcludeRegEx(String excludeRegEx) {
		this.excludeRegEx = excludeRegEx;
	}

	public String getCustomSourceImplClass() {
		return customSourceImplClass;
	}

	public void setCustomSourceImplClass(String customSourceImplClass) {
		this.customSourceImplClass = customSourceImplClass;
	}

	public long getStartTS() {
		return startTS;
	}

	public void setStartTS(long startTS) {
		this.startTS = startTS;
	}

	public long getEndTS() {
		return endTS;
	}

	public void setEndTS(long endTS) {
		this.endTS = endTS;
	}

	public long getMaxFilesize() {
		return maxFilesize;
	}

	public void setMaxFilesize(long maxFilesize) {
		this.maxFilesize = maxFilesize;
	}

	public long getMinFilesize() {
		return minFilesize;
	}

	public void setMinFilesize(long minFilesize) {
		this.minFilesize = minFilesize;
	}

	public boolean isDeleteSource() {
		return deleteSource;
	}

	public void setDeleteSource(boolean deleteSource) {
		this.deleteSource = deleteSource;
	}

	public boolean isIgnoreEmptyFiles() {
		return ignoreEmptyFiles;
	}

	public void setIgnoreEmptyFiles(boolean ignoreEmptyFiles) {
		this.ignoreEmptyFiles = ignoreEmptyFiles;
	}

	public long getCompressionThreshold() {
		return compressionThreshold;
	}

	public void setCompressionThreshold(long compressionThreshold) {
		this.compressionThreshold = compressionThreshold;
	}

	public ConnectionConfig getConnectionConfig() {
		return connectionConfig;
	}

	public void setConnectionConfig(ConnectionConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	public boolean isTransformSource() {
		return transformSource;
	}

	public void setTransformSource(boolean transformSource) {
		this.transformSource = transformSource;
	}

	public boolean isIncludeUpdatedFiles() {
		return includeUpdatedFiles;
	}

	public void setIncludeUpdatedFiles(boolean includeUpdatedFiles) {
		this.includeUpdatedFiles = includeUpdatedFiles;
	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

}
