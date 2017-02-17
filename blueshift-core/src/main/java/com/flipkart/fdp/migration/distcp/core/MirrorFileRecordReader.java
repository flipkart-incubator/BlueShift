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
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.BLUESHIFT_COUNTER;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.vo.FileTuple;

public class MirrorFileRecordReader extends RecordReader<Text, Text> {

	private String srcPath = null;
	private long progressByteCount = 0;

	private MirrorInputSplit fSplit = null;

	private DCMConfig dcmConfig = null;
	private Configuration conf = null;

	private List<FileTuple> inputs = null;
	private int index = 0;

	private InputStream in = null;
	private OutputStream out = null;

	private DCMCodec inCodec = null;
	private DCMCodec outCodec = null;

	private String taskId = null;

	private StateManager stateManager = null;
	private TransferStatus status = null;
	private Map<String, TransferStatus> transferStatus = null;
	private MD5Digester digest = null;

	private FileTuple current = null;

	private Text key = new Text();
	private Text value = new Text();
	private boolean read = false;
	private TaskAttemptContext context = null;

	@Override
	public void initialize(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {

		this.context = taskAttemptContext;
		this.conf = context.getConfiguration();
		dcmConfig = MirrorUtils.getConfigFromConf(conf);
		fSplit = (MirrorInputSplit) inputSplit;
		inputs = fSplit.getSplits();
		taskId = context.getTaskAttemptID().getTaskID().toString();

		stateManager = StateManagerFactory.getStateManager(conf, dcmConfig);

		transferStatus = stateManager.getTransferStatus(taskId);

		System.out.println("Initializing transfer of : " + inputs.size()
				+ " files, with a total Size of : " + fSplit.getLength());

		inCodec = DCMCodecFactory.getCodec(conf, fSplit.getSrcHostConfig());

		outCodec = DCMCodecFactory.getCodec(conf, fSplit.getDestHostConfig());

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (!read) {
			digest = null;
			current = inputs.get(index);
			srcPath = MirrorUtils.getSimplePath(new Path(current.getFileName()));
			status = checkTransferStatus();

			if (status == null) {

				status = new TransferStatus();
				status.setStatus(Status.NEW);

				status.setInputSize(current.getSize());
				status.setTs(current.getTs());
				status.setTaskID(taskId);
				System.out.println("Transfering file: " + current.getFileName()
						+ ", with size: " + current.getSize());

				try {
					analyzeStrategy();

					initializeStreams();

					digest = copy(in, out);

					status.setStatus(Status.COMPLETED);
					status.setMd5Digest(digest.getDigest());
					status.setOutputSize(digest.getByteCount());

					closeStreams();
				} catch (Exception e) {

					System.err.println("Error processing input: "
							+ e.getMessage());
					e.printStackTrace();
					status.setStatus(Status.FAILED);
					if (!dcmConfig.isIgnoreException()) {
						throw new IOException(e);
					}
				}
			}
			updateStatus();
			index++;

			if (index == inputs.size()) {
				context.setStatus("SUCCESS");
				read = true;
			}
			return true;
		} else {
			return false;
		}
	}

	public MD5Digester copy(InputStream input, OutputStream result)
			throws IOException {

		byte[] buffer = new byte[65536]; // 8K=8192 12K=12288 64K=65536
		long count = 0L;
		int n;

		long sts = System.currentTimeMillis();
		MD5Digester digester = new MD5Digester();

		while (-1 != (n = input.read(buffer))) {

			result.write(buffer, 0, n);

			digester.updateMd5digester(buffer, 0, n);
			count += n;

			if (count % 67108864 == 0) {
				System.out.println("Wrote 64M Data to Destination Total: "
						+ count + ", Time Taken(ms): "
						+ (System.currentTimeMillis() - sts));

				sts = System.currentTimeMillis();
				context.progress();
			}
			progressByteCount += count;
		}

		System.out.println("Transfer Complete Total: " + count
				+ ", Time Taken(ms): " + (System.currentTimeMillis() - sts));
		return digester;
	}

	private void initializeStreams() throws IOException {

		String destPath = srcPath;

		if(status.isInputTransformed()) {
			if (status.isInputCompressed()) {
				destPath = MirrorUtils.stripExtension(destPath);
			}
	
			if (status.isOutputCompressed()) {
				destPath = destPath + "."
						+ dcmConfig.getSinkConfig().getCompressionCodec();
			}
		} else {
			
		}
		
		String basePath = fSplit.getDestHostConfig().getPath();
		if (basePath != null && basePath.trim().length() > 1) {
			destPath = basePath + "/" + destPath;
		}
		basePath = dcmConfig.getSinkConfig().getPath();

		if (basePath != null && basePath.trim().length() > 1) {
			destPath = basePath + "/" + destPath;
		}

		status.setInputPath(srcPath);
		status.setOutputPath(destPath);

		if (!dcmConfig.getSinkConfig().isOverwriteFiles()) {
			if (outCodec.isExistsPath(destPath)) {
				throw new FileAlreadyExistsException(destPath);
			}
		}
		in = inCodec.createInputStream(srcPath, (status.isInputTransformed() && status.isInputCompressed()), status.isDecrypt(),status.getDecryptKey(),status.getDecryptIV());
		out = outCodec.createOutputStream(destPath
				+ DCMConstants.DCM_TEMP_EXTENSION, (status.isInputTransformed() && status.isOutputCompressed()),
				dcmConfig.getSinkConfig().getCompressionCodec(), dcmConfig
						.getSinkConfig().isAppend(),status.isEncrypt(),status.getEncryptKey(),status.getEncryptIV());

		String statusMesg = "Processing: " + srcPath + " -> " + destPath;
		context.setStatus(statusMesg);
		System.out.println("Status: " + statusMesg);
	}

	private TransferStatus checkTransferStatus() {

		TransferStatus stat = transferStatus.get(srcPath);
		if (stat != null && stat.getStatus() == Status.COMPLETED) {
			return stat;
		}
		return null;
	}

	private void updateStatus() throws IOException {

		//status.setOutputPath(fSplit.getDestHostConfig().getConnectionURL()
		//		+ "/" + status.getOutputPath());

		key.set(status.getInputPath());
		value.set(status.toString());

		if (status.getStatus() == Status.COMPLETED) {
			context.getCounter(BLUESHIFT_COUNTER.SUCCESS_COUNT).increment(1);
		} else {
			context.getCounter(BLUESHIFT_COUNTER.FAILED_COUNT).increment(1);
		}
		try {

			stateManager.updateTransferStatus(status);
		} catch (Exception e) {
			System.err.println("Caught exception persisting state: "
					+ e.getMessage());
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return read ? 1 : (((float) progressByteCount) / dcmConfig
				.getNumWorkers());
	}

	public void closeStreams() throws IOException {

		IOUtils.closeStream(in);
		IOUtils.closeStream(out);

		if (status.getStatus() == Status.COMPLETED) {

			outCodec.renameFile(status.getOutputPath()
					+ DCMConstants.DCM_TEMP_EXTENSION, status.getOutputPath());

			if (dcmConfig.getSourceConfig().isDeleteSource()) {
				try {
					inCodec.deleteSoureFile(srcPath);
				} catch (Exception e) {
					System.err.println("Failed Deleting file: " + srcPath
							+ ", Exception: " + e.getMessage());
				}
			}
		}
	}

	private void analyzeStrategy() {

		String srcCodec = MirrorUtils.getCodecNameFromPath(conf, srcPath);
		System.out.println("Source codec : " + srcCodec);
		if (srcCodec != null) {
			status.setInputCompressed(true);
		}
		
		if (dcmConfig.getSinkConfig().isUseCompression()) {
			status.setOutputCompressed(true);
		}
		
		//transformSource will have higher precedence over useCompression
		if(dcmConfig.getSourceConfig().isTransformSource()) {
			status.setInputTransformed(true);
		}
		if (dcmConfig.getSourceConfig().isDecrypt()) {
			status.setDecrypt(true);
			status.setDecryptKey(dcmConfig.getSourceConfig().getDecryptKey());
			status.setDecryptIV(dcmConfig.getSourceConfig().getDecryptIV());
		}
		if (dcmConfig.getSinkConfig().isEncrypt()) {
			status.setEncrypt(true);
			status.setEncryptKey(dcmConfig.getSinkConfig().getEncryptKey());
			status.setEncryptIV(dcmConfig.getSinkConfig().getEncryptIV());
		}
		if (!dcmConfig.getSourceConfig().isValidateTransfer()) {
			status.setValidateTransfer(false);
		}
	}

	@Override
	public void close() throws IOException {

		System.out.println("Transfer Complete...");
		IOUtils.closeStream(inCodec);
		IOUtils.closeStream(outCodec);
		IOUtils.closeStream(stateManager);
	}

}