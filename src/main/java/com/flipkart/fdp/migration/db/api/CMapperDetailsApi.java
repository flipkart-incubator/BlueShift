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
import com.flipkart.fdp.migration.db.core.CMapperDetailsDao;
import com.flipkart.fdp.migration.db.core.IMapperDetailsDao;
import com.flipkart.fdp.migration.db.models.MapperDetails;
import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.db.utils.EBase;

public class CMapperDetailsApi implements IMapperDetailsApi {

	private IMapperDetailsDao mapperDetailsDao;

	public CMapperDetailsApi(DBInitializer dbHelper) {
		this.mapperDetailsDao = dbHelper.getMapperDetailsDao();
	}

	@Override
	public MapperDetails createMapperDetails(long batchId, String taskId,
			String filePath, String digest, Status status) throws EBase {
		MapperDetails mapperDetails = new MapperDetails(batchId, filePath,
				status, digest, taskId);
		mapperDetailsDao.save(mapperDetails);
		return mapperDetails;
	}

	@Override
	public MapperDetails updateMapperDetails(long batchId, String taskId,
			String filePath, String digest, Status status) throws EBase {
		MapperDetails mapperDetails = new MapperDetails(batchId, filePath,
				status, digest, taskId);
		mapperDetailsDao.update(mapperDetails);
		return mapperDetails;
	}

	@Override
	public MapperDetails getMapperDetails(long batchId, String taskId)
			throws EBase {
		return mapperDetailsDao.getMapperDetails(batchId, taskId);
	}

	@Override
	public List<MapperDetails> getAllMapperDetails(long batchId) throws EBase {
		// return mapperDetailsDao.getByBatchId(batchId);
		return ((CMapperDetailsDao) mapperDetailsDao)
				.getMapperDetailsByBatchID(batchId);
	}

}
