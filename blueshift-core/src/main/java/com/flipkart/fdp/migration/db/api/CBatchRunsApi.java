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

package com.flipkart.fdp.migration.db.api;

import java.util.List;

import com.flipkart.fdp.migration.db.DBInitializer;
import com.flipkart.fdp.migration.db.core.IBatchRunsDao;
import com.flipkart.fdp.migration.db.models.BatchRun;
import com.flipkart.fdp.migration.db.utils.EBase;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;

public class CBatchRunsApi implements IBatchRunsApi {

	private IBatchRunsDao batchRunsDao;

	public CBatchRunsApi(DBInitializer dbHelper) {
		batchRunsDao = dbHelper.getBatchRunDao();
	}

	@Override
	public BatchRun createBatchRun(long batchId, String jobId,
			DCMConfig batchConfig, long startTime, long endTime, String trackingURL, Status status, String reason)
			throws EBase {
		BatchRun batchRun = new BatchRun(jobId, startTime, endTime, batchId,
				batchConfig.toString(), trackingURL, status, reason);
		batchRunsDao.save(batchRun);
		return batchRun;
	}

	@Override
	public BatchRun updateBatchRun(long batchId, String jobId,
			DCMConfig batchConfig, long startTime, long endTime, String trackingURL,  Status status, String reason)
			throws EBase {
		BatchRun batchRun = new BatchRun(jobId, startTime, endTime, batchId,
				batchConfig.toString(), trackingURL, status, reason);
		batchRunsDao.update(batchRun);
		return batchRun;
	}

	@Override
	public BatchRun getBatchRun(long batchId, String jobId) throws EBase {
		return batchRunsDao.getBatchRun(batchId, jobId);
	}

	@Override
	public List<BatchRun> getAllBatchRuns(long batchId) throws EBase {
		return batchRunsDao.getAllBatchRuns(batchId);
	}
}
