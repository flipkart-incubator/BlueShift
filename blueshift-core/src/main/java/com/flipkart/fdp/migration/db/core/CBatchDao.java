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

import lombok.extern.slf4j.Slf4j;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

import com.flipkart.fdp.migration.db.CServiceDao;
import com.flipkart.fdp.migration.db.models.Batch;
import com.flipkart.fdp.migration.db.utils.EBase;
import com.flipkart.fdp.migration.db.utils.EInvalid;
import com.flipkart.fdp.migration.db.utils.ENotFound;

@Slf4j
public class CBatchDao extends CServiceDao<Batch> implements IBatchDao {

	public CBatchDao(SessionFactory sessionFactory) {
		super(sessionFactory);
	}

	public Batch getById(long id) throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();

		try {
			log.debug("Getting a batch by id = {}", id);
			Batch entity = (Batch) session.get(Batch.class, id);
			if (entity == null) {
				throw new ENotFound("Id " + id + " not found for Batch");
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
}
