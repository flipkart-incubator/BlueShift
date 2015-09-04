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

package com.flipkart.fdp.migration.distcp.state;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;

/**
 * Created by sushil.s on 01/09/15.
 */
@Getter
@Setter
public class DummyStatemanager implements StateManager {

	private Configuration configuration = null;
	private DCMConfig dcmConfig = null;
	private String runId = null;
	private Path reportPath = null;

	public DummyStatemanager(Configuration conf, DCMConfig dcmConfig) {
		this.configuration = conf;
		this.dcmConfig = dcmConfig;
		this.runId = "ID_" + System.currentTimeMillis();

		Path batchBasePath = new Path(dcmConfig.getStatusPath() + "/"
				+ dcmConfig.getBatchName());

		Path runPath = new Path(batchBasePath, runId);
		reportPath = new Path(runPath, HDFSStateManager.REPORT_PATH);
	}

	@Override
	public void beginBatch() throws IOException {
	}

	@Override
	public void completeBatch(Status status) throws IOException {
	}

	@Override
	public void updateTransferStatus(TransferStatus status) throws IOException {
	}

	@Override
	public Map<String, TransferStatus> getTransferStatus(String taskId)
			throws IOException {
		return new HashMap<String, TransferStatus>();
	}

	@Override
	public void savePreiviousTransferStatus(
			Map<String, TransferStatus> prevState) throws IOException {
	}

	@Override
	public Map<String, TransferStatus> getPreviousTransferStatus()
			throws IOException {
		return new HashMap<String, TransferStatus>();
	}

	@Override
	public Path getReportPath() {
		return reportPath;
	}

	@Override
	public String getRunId() {
		return runId;
	}

	@Override
	public void close() throws IOException {
	}
}
