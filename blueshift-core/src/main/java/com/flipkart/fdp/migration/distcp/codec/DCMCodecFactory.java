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
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import org.apache.hadoop.conf.Configuration;
import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;

public class DCMCodecFactory {

    //TODO://Have to combine these methods to getCodec(..) , currently having this as MFTP is not supported for source.

	public static DCMCodec getSourceCodec(Configuration conf, ConnectionConfig config)
			throws IOException {
		try {
			String scheme = null;
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
			case CUSTOM:

			default:
				break;
			}
			if (scheme == null)
				throw new Exception("Unknown Filesystem, " + config.getType());

			return new GenericHadoopCodec(conf, config);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

    public static DCMCodec getDestinationCodec(Configuration conf, ConnectionConfig config,MirrorInputSplit inputSplit)
            throws IOException {
        try {
            String scheme = null;
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
                case MFTP:
                    return new GenericFTPCodec(conf, inputSplit);
                case CUSTOM:

                default:
                    break;
            }
            if (scheme == null)
                throw new Exception("Unknown Filesystem, " + config.getType());

            return new GenericHadoopCodec(conf,config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


}
