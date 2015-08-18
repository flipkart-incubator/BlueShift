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

package com.flipkart.fdp.migration.db;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.yammer.dropwizard.util.Generics.getTypeParameter;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

import com.flipkart.fdp.migration.db.utils.EBase;
import com.flipkart.fdp.migration.db.utils.EInvalid;

@Slf4j
@SuppressWarnings("unchecked")
public class CServiceDao<E> implements IServiceDao<E> {
	/**
	 * Creates a new DAO with a given session provider.
	 * 
	 * @param sessionFactory
	 *            a session provider
	 */
	private SessionFactory sessionFactory;
	private final Class<?> entityClass;

	public CServiceDao(SessionFactory sessionFactory) {
		this.sessionFactory = checkNotNull(sessionFactory);
		this.entityClass = getTypeParameter(getClass());
	}

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public Class<E> getEntityClass() {
		return (Class<E>) entityClass;
	}

	/**
	 * @param entity
	 *            Note that this object changes because
	 *            {@link Session#save(Object)} is impure, sir.
	 * @return the generated id, apparently
	 * @throws com.flipkart.fdp.migration.db.utils.EBase
	 */
	@Override
	public E save(E entity) throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		try {
			log.debug("Creating {} {}", getEntityClass().getName(), entity);
			session.save(entity);
			log.debug("{} Created Successfully {}", getEntityClass().getName(),
					entity);
			tx.commit();
			return entity;
		} catch (ConstraintViolationException e) {
			tx.rollback();
			throw new EInvalid(e);
		} finally {
			session.close();
		}
	}

	@Override
	public E update(E entity) throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		try {
			log.debug("Updating {} {}", getEntityClass().getName(), entity);
			session.update(entity);
			log.debug("{} Updated Successfully {}", getEntityClass().getName(),
					entity);
			tx.commit();
			return entity;
		} catch (ConstraintViolationException e) {
			tx.rollback();
			throw new EInvalid(e);
		} finally {
			session.close();
		}
	}

	@Override
	public List<E> getAll() throws EBase {
		Session session = getSessionFactory().openSession();
		Transaction tx = session.beginTransaction();
		List<E> list = new ArrayList<E>();
		try {
			list = (List<E>) session.createCriteria(getEntityClass()).list();
			tx.commit();
		} catch (Exception e) {
			tx.rollback();
			log.error("Exception while processing getAll : ", e);
			throw new EInvalid(e);
		} finally {
			session.close();
		}
		return list;
	}

	protected Session session() {
		return sessionFactory.getCurrentSession();
	}
}
