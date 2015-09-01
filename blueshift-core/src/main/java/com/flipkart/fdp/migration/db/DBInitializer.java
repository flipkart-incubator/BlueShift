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

import static org.hibernate.cfg.AvailableSettings.CURRENT_SESSION_CONTEXT_CLASS;
import static org.hibernate.cfg.AvailableSettings.DIALECT;
import static org.hibernate.cfg.AvailableSettings.DRIVER;
import static org.hibernate.cfg.AvailableSettings.HBM2DDL_AUTO;
import static org.hibernate.cfg.AvailableSettings.PASS;
import static org.hibernate.cfg.AvailableSettings.POOL_SIZE;
import static org.hibernate.cfg.AvailableSettings.RELEASE_CONNECTIONS;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.cfg.AvailableSettings.URL;
import static org.hibernate.cfg.AvailableSettings.USER;

import java.util.Properties;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;

import com.flipkart.fdp.migration.db.core.CBatchDao;
import com.flipkart.fdp.migration.db.core.CBatchRunsDao;
import com.flipkart.fdp.migration.db.core.CMapperDetailsDao;
import com.flipkart.fdp.migration.db.core.IBatchDao;
import com.flipkart.fdp.migration.db.core.IBatchRunsDao;
import com.flipkart.fdp.migration.db.core.IMapperDetailsDao;
import com.flipkart.fdp.migration.db.models.Batch;
import com.flipkart.fdp.migration.db.models.BatchRun;
import com.flipkart.fdp.migration.db.models.MapperDetails;
import com.flipkart.fdp.migration.distcp.config.DBConfig;

public class DBInitializer {

	private SessionFactory sessionFactory = null;

	private DBConfig config = null;

	private IBatchDao batchDao = null;

	private IBatchRunsDao batchRunDao = null;

	private IMapperDetailsDao mapperDetailsDao = null;

	public DBInitializer(DBConfig config) {
		this.config = config;
		sessionFactory = buildSessionFactory();

		batchDao = new CBatchDao(getSessionFactory());

		batchRunDao = new CBatchRunsDao(getSessionFactory());
		mapperDetailsDao = new CMapperDetailsDao(getSessionFactory());
	}

	private SessionFactory buildSessionFactory() {
		try {

			Configuration configuration = new Configuration();
			configuration.setProperties(getHibernateProperties());
			configuration.addAnnotatedClass(Batch.class);
			configuration.addAnnotatedClass(BatchRun.class);
			configuration.addAnnotatedClass(MapperDetails.class);

			StandardServiceRegistryBuilder serviceRegistryBuilder = new StandardServiceRegistryBuilder();
			serviceRegistryBuilder.applySettings(configuration.getProperties());
			ServiceRegistry serviceRegistry = serviceRegistryBuilder.build();
			sessionFactory = configuration.buildSessionFactory(serviceRegistry);

			return sessionFactory;
		} catch (Throwable e) {
			System.err.println("Initial SessionFactory creation failed." + e);
			throw new ExceptionInInitializerError(e);
		}
	}

	private Properties getHibernateProperties() {
		Properties properties = new Properties();
		properties.put(DRIVER, config.getDbDriver());
		properties.put(USER, config.getDbUserName());
		properties.put(PASS, config.getDbUserPassword());
		properties.put(URL, config.getDbConnectionURL());
		properties.put(DIALECT, config.getDbDialect());
		// "org.hibernate.dialect.MySQL5Dialect"
		properties.put(SHOW_SQL, "false");
		properties.put(HBM2DDL_AUTO, "update");
		properties.put(CURRENT_SESSION_CONTEXT_CLASS, "thread");
		// properties.put("current_session_context_class", "thread");
		properties.put(RELEASE_CONNECTIONS, "after_transaction");

		properties.put(POOL_SIZE, 10);
		properties.put(org.hibernate.cfg.AvailableSettings.CONNECTION_PROVIDER,
				"org.hibernate.connection.C3P0ConnectionProvider");
		properties.put(org.hibernate.cfg.AvailableSettings.C3P0_MAX_SIZE, 100);
		properties.put(org.hibernate.cfg.AvailableSettings.C3P0_MIN_SIZE, 5);
		properties.put(org.hibernate.cfg.AvailableSettings.C3P0_MAX_STATEMENTS,
				0);
		properties.put(
				org.hibernate.cfg.AvailableSettings.C3P0_ACQUIRE_INCREMENT, 1);
		properties.put(
				org.hibernate.cfg.AvailableSettings.C3P0_IDLE_TEST_PERIOD, 100);
		properties.put(org.hibernate.cfg.AvailableSettings.C3P0_TIMEOUT, 100);

		return properties;
	}

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}

	public void shutdown() {
		getSessionFactory().close();
	}

	public IBatchDao getBatchDao() {
		return batchDao;
	}

	public IBatchRunsDao getBatchRunDao() {
		return batchRunDao;
	}

	public IMapperDetailsDao getMapperDetailsDao() {
		return mapperDetailsDao;
	}
}
