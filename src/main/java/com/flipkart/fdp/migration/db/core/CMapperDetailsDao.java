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

package com.flipkart.fdp.migration.db.core;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

import com.flipkart.fdp.migration.db.CServiceDao;
import com.flipkart.fdp.migration.db.models.MapperDetails;
import com.flipkart.fdp.migration.db.utils.EBase;
import com.flipkart.fdp.migration.db.utils.EFailure;
import com.flipkart.fdp.migration.db.utils.EInvalid;
import com.flipkart.fdp.migration.db.utils.ENotFound;

@Slf4j
public class CMapperDetailsDao extends CServiceDao<MapperDetails> implements
		IMapperDetailsDao {

	public CMapperDetailsDao(SessionFactory sessionFactory) {
		super(sessionFactory);
	}

	@Override
	public MapperDetails getMapperDetails(long batchId, String filePath)
			throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		try {
			Query query = session
					.createQuery(
							"FROM MapperDetails where batchId = :batchId AND file_path = :filePath")
					.setParameter("batchId", batchId)
					.setParameter("filePath", filePath);
			MapperDetails mapperDetails = (MapperDetails) query.uniqueResult();
			tx.commit();
			return mapperDetails;
		} catch (Exception e) {
			tx.rollback();
			log.error("Exception while processing getByRunId : ", e);
			throw new EFailure(e);
		} finally {
			session.close();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<MapperDetails> getByBatchId(long batchId) throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		try {
			log.debug("Getting a batch by id = {}", batchId);
			List<MapperDetails> entity = (List<MapperDetails>) session.get(
					MapperDetails.class, batchId);
			if (entity == null) {
				throw new ENotFound("Id " + batchId
						+ " not found for MapperDetails");
			}
			tx.commit();
			return entity;
		} catch (ConstraintViolationException e) {
			tx.rollback();
			throw new EInvalid(e);
		} finally {
			session.close();
		}
	}

	public List<MapperDetails> getMapperDetailsByBatchID(long batchId)
			throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		try {
			Query query = session.createQuery(
					"FROM MapperDetails where batchId = :batchId")
					.setParameter("batchId", batchId);

			List<MapperDetails> mapperDetails = (List<MapperDetails>) query
					.list();
			tx.commit();
			return mapperDetails;
		} catch (Exception e) {
			tx.rollback();
			log.error("Exception while processing getByRunId : ", e);
			throw new EFailure(e);
		} finally {
			session.close();
		}
	}

}
