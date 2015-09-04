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

package com.flipkart.fdp.migration.distcp.codec;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.flipkart.fdp.migration.distcp.codec.optimizer.SingleSinkOptimizer;
import com.flipkart.fdp.migration.distcp.codec.optimizer.MultiSinkOptimizer;
import com.flipkart.fdp.migration.distcp.codec.optimizer.WorkloadOptimizer;
import com.flipkart.fdp.migration.distcp.config.ConnectableConfig;
import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.SinkConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.DCMCodecType;

public class DCMCodecFactory {

	public static DCMCodec getCodec(Configuration conf, ConnectionConfig config)
			throws IOException {

		FileSystem fs = null;

		try {
			URI connectionURI = new URI(config.getConnectionURL());
			String scheme = connectionURI.getScheme().toLowerCase();

			if ("webhdfs".equals(scheme)) {
				fs = getFilesystem(conf, config, config.getConnectionURL());
			} else if ("hdfs".equals(scheme)) {
				fs = getFilesystem(conf, config, config.getConnectionURL());
			} else if ("har".equals(scheme)) {
				fs = getFilesystem(conf, config, config.getConnectionURL());
			} else if ("ftp".equals(scheme)) {
				fs = getFilesystem(conf, config, config.getConnectionURL());
			} else if ("hftp".equals(scheme)) {
				fs = getFilesystem(conf, config, config.getConnectionURL());
			} else if ("mftp".equals(scheme)) {
				String uri = config.getConnectionURL().replaceFirst("mftp",
						"ftp");
				fs = getFilesystem(conf, config, uri);
			} else {

				throw new Exception("Unknown Filesystem scheme, " + scheme);
			}
			return new GenericHadoopCodec(conf, config, fs);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static FileSystem getFilesystem(Configuration conf,
			ConnectionConfig config, String fsURI) throws Exception {

		if (config.getSecurityType() == DCMConstants.SecurityType.KERBEROS)
			return FileSystem.newInstance(new URI(fsURI), conf);
		else
			return FileSystem.newInstance(new URI(fsURI), conf,
					config.getUserName());
	}

	public static WorkloadOptimizer getCodecWorkloadOptimizer(
			ConnectionConfig config) throws Exception {

		URI connectionURI = new URI(config.getConnectionURL());
		String scheme = connectionURI.getScheme().toLowerCase();

		if ("webhdfs".equals(scheme)) {
			return new SingleSinkOptimizer();
		} else if ("hdfs".equals(scheme)) {
			return new SingleSinkOptimizer();
		} else if ("har".equals(scheme)) {
			return new SingleSinkOptimizer();
		} else if ("ftp".equals(scheme)) {
			return new SingleSinkOptimizer();
		} else if ("hftp".equals(scheme)) {
			return new SingleSinkOptimizer();
		} else if ("mftp".equals(scheme)) {
			return new MultiSinkOptimizer();
		} else {

			throw new Exception("Unknown Filesystem scheme, " + scheme);
		}
	}

	public static WorkloadOptimizer getSinkCodecWorkloadOptimizer(
			SinkConfig config) throws Exception {

		return null;

	}

	public DCMCodecType getCodecType(ConnectableConfig config) {

		if (config.getConnectionConfig().size() > 1)
			return DCMCodecType.MULTI;
		else
			return DCMCodecType.SINGLE;

	}
}
