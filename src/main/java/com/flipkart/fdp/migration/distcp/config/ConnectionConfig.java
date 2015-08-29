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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.FileSystemType;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.SecurityType;
import com.google.gson.Gson;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionConfig {

	private FileSystemType type = null;


	private HostConfig hostConfig = null;

    private List<HostConfig> hostConfigList = null;
    private String connectionParams = null;

    public String getConnectionParams() {
        return connectionParams;
    }

    public void setConnectionParams(String connectionParams) {
        this.connectionParams = connectionParams;
    }


    public HostConfig getHostConfig() {
        return hostConfig;
    }

    public void setHostConfig(HostConfig hostConfig) {
        this.hostConfig = hostConfig;
    }

    public List<HostConfig> getHostConfigList() {
        return hostConfigList;
    }

    public void setHostConfigList(List<HostConfig> hostConfigList) {
        this.hostConfigList = hostConfigList;
    }

    public FileSystemType getType() {
		return type;
	}

	public void setType(FileSystemType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

}
