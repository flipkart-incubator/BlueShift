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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.flipkart.fdp.migration.db.DBInitializer;
import com.flipkart.fdp.migration.db.api.CBatchApi;
import com.flipkart.fdp.migration.db.api.CBatchRunsApi;
import com.flipkart.fdp.migration.db.api.CMapperDetailsApi;
import com.flipkart.fdp.migration.db.models.Batch;
import com.flipkart.fdp.migration.db.models.MapperDetails;
import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;

public class MySqlStateManager implements StateManager {

	private Configuration configuration = null;
	private DCMConfig dcmConfig = null;
	private Path reportPath = null;

	private DBInitializer dbHelper = null;
	private CBatchApi batchAPI = null;
	private CBatchRunsApi batchRunsAPI = null;
	private CMapperDetailsApi mapDetails = null;

	private Batch batch = null;

	private String runId = null;

	private long startTime = 0, endTime = 0;

	public MySqlStateManager(Configuration conf, DCMConfig config)
			throws IOException {

		this.configuration = conf;
		this.dcmConfig = config;
		runId = configuration.get(HDFSStateManager.RUN_ID);

		if (runId == null) {
			runId = String.valueOf(System.currentTimeMillis());
			configuration.set(HDFSStateManager.RUN_ID, runId);
		}
		dbHelper = new DBInitializer(dcmConfig.getDbConfig());
		batchAPI = new CBatchApi(dbHelper);
		batchRunsAPI = new CBatchRunsApi(dbHelper);
		mapDetails = new CMapperDetailsApi(dbHelper);

		Path batchBasePath = new Path(dcmConfig.getStatusPath() + "/"
				+ dcmConfig.getBatchName());

		Path runPath = new Path(batchBasePath, runId);
		reportPath = new Path(runPath, HDFSStateManager.REPORT_PATH);
	}

	@Override
	public void beginBatch() throws IOException {
		startTime = System.currentTimeMillis();
		try {
			batch = batchAPI.getBatch(dcmConfig.getBatchID());
		} catch (Exception e) {
			System.out.println("Error getting batch Details from DB: "
					+ e.getMessage());
			batch = null;
		}

		try {
			if (batch == null) {
				batchAPI.createBatch(dcmConfig.getBatchID(),
						dcmConfig.getBatchName(), "0",
						System.currentTimeMillis() + "", Status.NEW, false);
			}
			batch = batchAPI.getBatch(dcmConfig.getBatchID());
			if (batch.isLock()) {
				throw new Exception(
						"Batch is locked, Manually release the lock or Please ensure no other execution is happening on this batch...");
			} else {
				batchAPI.updateBatch(dcmConfig.getBatchID(),
						dcmConfig.getBatchName(), batch.getLastRunJobId(),
						System.currentTimeMillis() + "", Status.NEW, true);
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public void completeBatch(Status status) throws IOException {

		endTime = System.currentTimeMillis();
		try {
			batchAPI.updateBatch(dcmConfig.getBatchID(),
					dcmConfig.getBatchName(), runId, batch.getDesc(), status,
					false);

			batchRunsAPI.createBatchRun(dcmConfig.getBatchID(), runId,
					dcmConfig, startTime, endTime, status);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void savePreiviousTransferStatus(
			Map<String, TransferStatus> prevState) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, TransferStatus> getPreviousTransferStatus()
			throws IOException {

		Map<String, TransferStatus> status = new HashMap<String, TransferStatus>();
		try {
			List<MapperDetails> maps = mapDetails.getAllMapperDetails(dcmConfig
					.getBatchID());
			if (maps == null || maps.size() <= 0)
				return status;
			for (MapperDetails map : maps) {
				status.put(map.getSrcPath(),
						getTransfreStatusFromMapDetails(map));
			}
		} catch (Exception e) {
			System.out
					.println("Exception fetching previous map details from DB.");
			throw new IOException(e);
		}
		return status;
	}

	public TransferStatus getTransfreStatusFromMapDetails(MapperDetails map) {
		TransferStatus tstat = new TransferStatus();
		tstat.setInputPath(map.getSrcPath());
		tstat.setInputSize(map.getSrcSize());
		tstat.setOutputPath(map.getDestPath());
		tstat.setOutputSize(map.getDestSize());
		tstat.setMd5Digest(map.getDigest());
		tstat.setStatus(map.getStatus());
		tstat.setTaskID(map.getTaskId());
		tstat.setTs(map.getTs());
		return tstat;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void updateTransferStatus(TransferStatus status) throws IOException {
		try {
			mapDetails.createMapperDetails(dcmConfig.getBatchID(), status);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public Map<String, TransferStatus> getTransferStatus(String taskId)
			throws IOException {
		Map<String, TransferStatus> status = new HashMap<String, TransferStatus>();
		try {
			List<MapperDetails> maps = mapDetails.getAllMapperDetailsForTask(
					dcmConfig.getBatchID(), taskId);
			if (maps == null || maps.size() <= 0)
				return status;
			for (MapperDetails map : maps) {
				status.put(map.getSrcPath(),
						getTransfreStatusFromMapDetails(map));
			}
		} catch (Exception e) {
			System.out
					.println("Exception fetching previous map details from DB.");
			throw new IOException(e);
		}
		return status;
	}

	@Override
	public Path getReportPath() {
		return reportPath;
	}

	@Override
	public String getRunId() {
		return runId;
	}
}
