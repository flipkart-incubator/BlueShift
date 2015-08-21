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

import static junit.framework.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.flipkart.fdp.migration.db.DBInitializer;
import com.flipkart.fdp.migration.db.models.Batch;
import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.db.utils.EBase;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;

public class IBatchApiTest {
	private static IBatchApi batchApi;
	private static DBInitializer dbhelper = null;

	@BeforeClass
	public static void setUp() throws Exception {
		batchApi = new CBatchApi(dbhelper);
	}

	@Test
	public void testBatchCreation() throws EBase {
		long batchId = 1234;
		String batchName = "Ad";
		String lastRunJobId = "Job12";
		DCMConfig mirrorDCMConfig = new DCMConfig();
		String batchConfig = mirrorDCMConfig.toString();
		Status status = Status.STARTED;
		Batch batch = new Batch(batchId, batchName, lastRunJobId, batchConfig,
				status, false);
		Batch createdBatch = batchApi.createBatch(batchId, batchName,
				lastRunJobId, "test", status, true);
		assertEquals(batch.getDesc(), createdBatch.getDesc());
		assertEquals(batch.getBatchId(), createdBatch.getBatchId());
		assertEquals(batch.getBatchName(), createdBatch.getBatchName());
		assertEquals(batch.getLastRunJobId(), createdBatch.getLastRunJobId());
		assertEquals(batch.getStatus(), createdBatch.getStatus());
	}
}
