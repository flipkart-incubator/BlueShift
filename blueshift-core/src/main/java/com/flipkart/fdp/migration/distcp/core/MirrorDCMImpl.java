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

package com.flipkart.fdp.migration.distcp.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.BLUESHIFT_COUNTER;
import com.flipkart.fdp.migration.distcp.config.SinkConfig;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.vo.FileTuple;
import com.google.gson.Gson;

public class MirrorDCMImpl {

	public static class MirrorMapper extends Mapper<Text, Text, Text, Text> {

		private Configuration conf = null;
		private DCMCodec codec = null;
		private Gson gson = null;
		private Text output = new Text();
		
		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			this.conf = context.getConfiguration();
			DCMConfig dcmConfig = MirrorUtils.getConfigFromConf(conf);
			SinkConfig sinkConfig = dcmConfig.getSinkConfig();
			this.codec = DCMCodecFactory.getCodec(conf, sinkConfig.getDefaultConnectionConfig());
			this.gson = new Gson();
		}
		
		/*
		 * Do verification of each file by re-computing md5Hash
		 * Also add md5Hash in transfer status and send it to reducer.
		 */
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			TransferStatus transferStatus = this.gson.fromJson(value.toString(), TransferStatus.class);
			if (transferStatus.isValidateTransfer()) {
				String fileName = transferStatus.getOutputPath();
				String md5DigestInput = transferStatus.getMd5Digest();
				InputStream inputStream = null;
				try {
					inputStream = codec.createInputStream(fileName, (transferStatus.isInputTransformed() && transferStatus.isOutputCompressed()), transferStatus.isEncrypt(), transferStatus.getEncryptKey(), transferStatus.getEncryptIV());
					String md5DigestOutput = computeMd5(inputStream);
					transferStatus.setMd5DigestOutput(md5DigestOutput);
					if (md5DigestInput.equals(md5DigestOutput)) {
						context.getCounter(BLUESHIFT_COUNTER.VERIFIED_SUCCESS_COUNT).increment(1);
					} else {
						System.out.println("Verification failed for : " + fileName + ", md5DigestInput : [" + md5DigestInput
								+ "], md5DigestOutput : [" + md5DigestOutput + "]");
						context.getCounter(BLUESHIFT_COUNTER.VERIFIED_FAILED_COUNT).increment(1);
						transferStatus.setStatus(DCMConstants.Status.FAILED);
					}
				} catch (Exception e) {
					context.getCounter(BLUESHIFT_COUNTER.VERIFIED_FAILED_COUNT).increment(1);
					transferStatus.setStatus(DCMConstants.Status.FAILED);
					System.out.println("Caught Exception : " + e.getMessage());
					e.printStackTrace();
				} finally {
					if (inputStream != null) {
						inputStream.close();
					}
				}

			}
			output.set(transferStatus.toString());
			context.write(key, output);
		}
		
		private String computeMd5(InputStream inputStream) throws IOException {
			
			byte[] buffer = new byte[65536]; // 8K=8192 12K=12288 64K=65536
			int n;

			MD5Digester digester = new MD5Digester();

			while (-1 != (n = inputStream.read(buffer))) {
				digester.updateMd5digester(buffer, 0, n);
			}
			return digester.getDigest();
		}
	}

	public static class MirrorReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value,null);
			}
		}
	}

	public static class FileTupleComparator implements Comparator<FileTuple> {

		// @Override
		public int compare(FileTuple f0, FileTuple f1) {

			if (f1.getSize() > f0.getSize())
				return 1;
			if (f1.getSize() < f0.getSize())
				return -1;
			return 0;
		}

	}

	public static void HackMapreduce() throws Exception {

		DCMConstants.setFinalStatic(
				org.apache.hadoop.mapreduce.lib.input.FileInputFormat.class
						.getDeclaredField("hiddenFileFilter"),
				new PathFilter() {
					public boolean accept(Path p) {
						return true;
					}
				});
		DCMConstants.setFinalStatic(
				org.apache.hadoop.mapred.FileInputFormat.class
						.getDeclaredField("hiddenFileFilter"),
				new PathFilter() {
					public boolean accept(Path p) {
						return true;
					}
				});
	}

}
