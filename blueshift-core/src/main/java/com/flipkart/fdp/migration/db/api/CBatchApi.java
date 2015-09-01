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
import com.flipkart.fdp.migration.db.core.IBatchDao;
import com.flipkart.fdp.migration.db.models.Batch;
import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.db.utils.EBase;

public class CBatchApi implements IBatchApi {
	private IBatchDao batchDao;

	public CBatchApi(DBInitializer dbHelper) {
		this.batchDao = dbHelper.getBatchDao();
	}

	@Override
	public Batch createBatch(long batchId, String batchName,
			String lastRunJobId, String desc, Status status, boolean lock)
			throws EBase {
		Batch batch = new Batch(batchId, batchName, lastRunJobId, desc, status,
				lock);
		batchDao.save(batch);
		return batch;
	}

	@Override
	public Batch updateBatch(long batchId, String batchName,
			String lastRunJobId, String desc, Status status, boolean lock)
			throws EBase {
		Batch batch = new Batch(batchId, batchName, lastRunJobId, desc, status,
				lock);
		batchDao.update(batch);
		return batch;
	}

	@Override
	public Batch getBatch(long batchId) throws EBase {
		return batchDao.getById(batchId);
	}

	@Override
	public List<Batch> getAllBatches() throws EBase {
		return batchDao.getAll();
	}
}
