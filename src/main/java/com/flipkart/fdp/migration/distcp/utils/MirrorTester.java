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

package com.flipkart.fdp.migration.distcp.utils;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DBConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.FileSystemType;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.SecurityType;
import com.flipkart.fdp.migration.distcp.config.SinkConfig;
import com.flipkart.fdp.migration.distcp.config.SourceConfig;

public class MirrorTester {

	public static void main(String[] args) throws Exception {
		TestDriver();

	}

	private static void TestDriver() throws Exception {

		ConnectionConfig conconfig = new ConnectionConfig();
		conconfig.setKeyFile("test");
		conconfig.setConnectionParams("asdfasd");
		conconfig.setSecurityType(SecurityType.SIMPLE);
		conconfig.setHost("files.religare.in");
		conconfig.setPort(20);
		conconfig.setType(FileSystemType.FTP);
		conconfig.setUserName("fk-bigfoot-azkaban");
		conconfig.setUserPassword("fk-bigfoot-azkaban");
		System.out.println(conconfig.toString());

		DCMCodec codec = DCMCodecFactory.getCodec(new Configuration(),
				conconfig);
		List<FileStatus> stats = codec.getInputPaths("/tmp/raj");
		for (FileStatus stat : stats) {
			System.out.println(stat);
		}
	}

	public static void CreateConfigTest() {
		// TestDriver();
		DCMConfig config = new DCMConfig();

		config.setBatchName("Test");
		config.setIgnoreException(false);
		config.setNumWorkers(10);
		config.setStatusPath("/tmp");

		DBConfig dconfig = new DBConfig();
		dconfig.setDbConnectionURL("/test");
		dconfig.setDbDialect("test");
		dconfig.setDbDriver("driver");
		dconfig.setDbUserName("name");
		dconfig.setDbUserPassword("pass");
		config.setDbConfig(dconfig);

		SourceConfig sconfig = new SourceConfig();
		sconfig.setCompressionThreshold(100);

		sconfig.setCustomSourceImplClass("test");
		sconfig.setDeleteSource(false);
		sconfig.setStartTS(0);
		sconfig.setEndTS(0);
		sconfig.setExcludeListFile("test");
		sconfig.setExcludeRegEx("test");
		sconfig.setIgnoreEmptyFiles(false);
		sconfig.setIncludeListFile("test");
		sconfig.setIncludeRegEx("test");
		sconfig.setMaxFilesize(0);
		sconfig.setMinFilesize(0);
		sconfig.setPath("test");

		ConnectionConfig conconfig = new ConnectionConfig();
		conconfig.setKeyFile("test");
		conconfig.setConnectionParams("asdfasd");
		conconfig.setSecurityType(SecurityType.PSEUDO);
		conconfig.setHost("test");
		conconfig.setPort(12);
		conconfig.setType(FileSystemType.HDFS);
		conconfig.setUserName("name");
		conconfig.setUserPassword("pass");
		sconfig.setConnectionConfig(conconfig);

		config.setSourceConfig(sconfig);

		SinkConfig iconfig = new SinkConfig();
		iconfig.setAppend(false);
		iconfig.setCompressionCodec("snappy");
		iconfig.setCustomSinkImplClass("ksks");
		iconfig.setPath("path");
		iconfig.setOverwriteFiles(false);
		iconfig.setUseCompression(false);
		iconfig.setConnectionConfig(conconfig);
		config.setSinkConfig(iconfig);

		System.out.println(config.toString());

	}

}
