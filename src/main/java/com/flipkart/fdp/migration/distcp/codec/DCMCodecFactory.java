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

import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.SecurityType;

public class DCMCodecFactory {

	public static DCMCodec getCodec(Configuration conf, ConnectionConfig config)
			throws IOException {
		try {
			String scheme = null;
			FileSystem fs = null;
			switch (config.getType()) {

			case WEBHDFS:
				scheme = DCMConstants.WEBHDFS_DEFAULT_PROTOCOL;
				break;
			case HDFS:
				scheme = DCMConstants.HDFS_DEFAULT_PROTOCOL;
				break;
			case HFTP:
				scheme = DCMConstants.HFTP_DEFAULT_PROTOCOL;
				break;
			case HAR:
				scheme = DCMConstants.HAR_DEFAULT_PROTOCOL;
				break;
			case FTP:
				scheme = DCMConstants.FTP_DEFAULT_PROTOCOL;
				break;
			case CUSTOM:

			default:
				break;
			}
			if (scheme == null)
				throw new Exception("Unknown Filesystem, " + config.getType());

			if (fs == null)
				fs = getHadoopFilesystem(scheme, conf, config);
			return new GenericHadoopCodec(fs);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static FileSystem getHadoopFilesystem(String scheme,
			Configuration conf, ConnectionConfig config) throws Exception {

		String httpfsUrl = scheme + config.getHostConfig().getHost() + ":" + config.getHostConfig().getPort();

		if (config.getHostConfig().getSecurityType() == SecurityType.KERBEROS)
			return FileSystem.newInstance(new URI(httpfsUrl), conf);
		else
			return FileSystem.newInstance(new URI(httpfsUrl), conf,
					config.getHostConfig().getUserName());
	}

}
