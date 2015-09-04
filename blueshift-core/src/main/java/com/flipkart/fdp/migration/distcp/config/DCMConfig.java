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

import com.flipkart.fdp.migration.distcp.config.DCMConstants.StateManagerImpl;
import com.google.gson.Gson;

public class DCMConfig {

	private String batchName = null;

	private int numWorkers = 0;

	private boolean ignoreException = false;

	private boolean localModeExecution = false;

	private int maxRetries = 0;

	private String statusPath = null;

	private StateManagerImpl stateManagerType = null;

	private SourceConfig sourceConfig = null;

	private SinkConfig sinkConfig = null;

	private DBConfig dbConfig = null;

	public long getBatchID() {
		return DCMConstants.HASH_MEDIAN
				+ DCMConstants.hasher.hashBytes(
						batchName.toLowerCase().getBytes()).asInt();
	}

	public String getBatchName() {
		return batchName;
	}

	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}

	public boolean isIgnoreException() {
		return ignoreException;
	}

	public void setIgnoreException(boolean ignoreException) {
		this.ignoreException = ignoreException;
	}

	public String getStatusPath() {
		return statusPath;
	}

	public void setStatusPath(String statusPath) {
		this.statusPath = statusPath;
	}

	public SourceConfig getSourceConfig() {
		return sourceConfig;
	}

	public void setSourceConfig(SourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	public SinkConfig getSinkConfig() {
		return sinkConfig;
	}

	public void setSinkConfig(SinkConfig sinkConfig) {
		this.sinkConfig = sinkConfig;
	}

	public DBConfig getDbConfig() {
		return dbConfig;
	}

	public void setDbConfig(DBConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

	public int getNumWorkers() {
		return numWorkers;
	}

	public void setNumWorkers(int numWorkers) {
		this.numWorkers = numWorkers;
	}

	public StateManagerImpl getStateManagerType() {
		return stateManagerType;
	}

	public void setStateManagerType(StateManagerImpl stateManagerType) {
		this.stateManagerType = stateManagerType;
	}

	public boolean isLocalModeExecution() {
		return localModeExecution;
	}

	public void setLocalModeExecution(boolean localModeExecution) {
		this.localModeExecution = localModeExecution;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

}
