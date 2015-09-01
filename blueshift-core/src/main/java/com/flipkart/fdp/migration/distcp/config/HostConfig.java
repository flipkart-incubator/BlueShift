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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;

/**
 * Created by sushil.s on 28/08/15.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class HostConfig implements Writable {
	private String userName;
	private String userPassword;
	private String keyFile;
	private String host;
	private int port;
	private DCMConstants.SecurityType securityType;
	private long freeSpaceInBytes;
	private String destPath;

	public HostConfig(HostConfig hostConfig) {
		this.userName = hostConfig.getUserName();
		this.userPassword = hostConfig.getUserPassword();
		this.keyFile = hostConfig.getKeyFile();
		this.port = hostConfig.getPort();
		this.securityType = hostConfig.getSecurityType();
		this.freeSpaceInBytes = hostConfig.getFreeSpaceInBytes();
		this.destPath = hostConfig.getDestPath();
		this.host = hostConfig.getHost();
	}

	public int compareTo(HostConfig o) {
		Long l = getFreeSpaceInBytes();
		if (o == null)
			return l.compareTo(0L);
		return l.compareTo(o.getFreeSpaceInBytes());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, getUserName());
		Text.writeString(out, getUserPassword());
		Text.writeString(out, getKeyFile());
		Text.writeString(out, getHost());
		out.writeInt(getPort());
		// Text.writeString(out, String.valueOf(securityType)); //Todo://
		// Uncomment this after we enable Security
		out.writeLong(getFreeSpaceInBytes());
		Text.writeString(out, getDestPath());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		userName = Text.readString(in);
		userPassword = Text.readString(in);
		keyFile = Text.readString(in);
		host = Text.readString(in);
		port = in.readInt();
		securityType = DCMConstants.SecurityType.SIMPLE;
		freeSpaceInBytes = in.readLong();
		destPath = Text.readString(in);

	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
