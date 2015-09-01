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
package com.flipkart.fdp.migration.distcp.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.flipkart.fdp.migration.FSclients.MultiFTPClient;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.HostConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;

/**
 * Created by sushil.s on 31/08/15.
 */
public class GenericFTPCodec implements DCMCodec {

	private HostConfig hostConfig = null;
	private MultiFTPClient multiFTPClient = null;
	private long totalBatchSize = 0l;
	private Configuration conf = null;

	public GenericFTPCodec(Configuration conf, HostConfig hostConfig)
			throws Exception {
		this.conf = conf;
		this.hostConfig = hostConfig;
	}

	private URI constructFtpURLString(String destFileName)
			throws URISyntaxException {
		/*
		 * System.out.println("URI : "+String.valueOf(
		 * DCMConstants.FTP_DEFAULT_PROTOCOL + hostConfig.getUserName() + ":" +
		 * hostConfig.getUserPassword() + "@" + hostConfig.getHost() +
		 * hostConfig.getDestPath() + destFileName));
		 */
		return new URI(String.valueOf(DCMConstants.FTP_DEFAULT_PROTOCOL
				+ hostConfig.getUserName() + ":" + hostConfig.getUserPassword()
				+ "@" + hostConfig.getHost() + hostConfig.getDestPath()
				+ destFileName));
	}

	@Override
	public OutputStream createOutputStream(Configuration conf, String path,
			boolean append) throws IOException {
		try {
			String[] pathTree = path.split("/");
			multiFTPClient = new MultiFTPClient(
					constructFtpURLString(pathTree[pathTree.length - 1]), conf);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return multiFTPClient.getOutputStream();
	}

	@Override
	public InputStream createInputStream(Configuration conf, String path)
			throws IOException {
		throw new IOException(
				"Creating Inputstream for Multi-FTP not supported!");
	}

	@Override
	public boolean deleteSoureFile(String path) throws IOException {
		return false;
	}

	@Override
	public boolean isSplitable() {
		return false;
	}

	@Override
	public boolean isExistsPath(String path) throws IOException {
		return false;
	}

	@Override
	public List<MirrorDCMImpl.FileTuple> getInputPaths(String path,
			Collection<String> excludeList) throws Exception {
		return null;
	}

	@Override
	public List<MirrorDCMImpl.FileTuple> getInputPaths(
			Collection<String> paths, Collection<String> excludeList)
			throws Exception {
		return null;
	}

	@Override
	public void close() throws IOException {

	}

	public MultiFTPClient getMultiFTPClient() {
		return multiFTPClient;
	}

	public void setMultiFTPClient(MultiFTPClient multiFTPClient) {
		this.multiFTPClient = multiFTPClient;
	}

	public long getTotalBatchSize() {
		return totalBatchSize;
	}

	public void setTotalBatchSize(long totalBatchSize) {
		this.totalBatchSize = totalBatchSize;
	}

	public HostConfig getHostConfig() {
		return hostConfig;
	}

	public void setHostConfig(HostConfig hostConfig) {
		this.hostConfig = hostConfig;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
