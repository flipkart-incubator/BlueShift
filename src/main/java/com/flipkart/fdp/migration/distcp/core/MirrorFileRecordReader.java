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

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.MD5Digester;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.BLUESHIFT_COUNTER;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;

public class MirrorFileRecordReader extends RecordReader<Text, Text> {

	private boolean read = false;

	private String srcPath = null;

	private Text key = new Text();
	private Text value = new Text();

	private MirrorInputSplit fSplit = null;

	private DCMConfig dcmConfig = null;
	private Configuration conf = null;
	private StateManager stateManager = null;

	private List<FileTuple> inputs = null;
	private int index = 0;
	private FileTuple current = null;

	private InputStream in = null;
	private OutputStream out = null;

	private DCMCodec inCodec = null;
	private DCMCodec outCodec = null;

	private TransferStatus status = null;

	private Map<String, TransferStatus> transferStatus = null;

	private MD5Digester digest = null;

	private String taskId = null;

	private TaskAttemptContext context = null;

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
		return read ? 1 : (((float) index) / inputs.size());
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		this.context = context;
		this.conf = context.getConfiguration();
		dcmConfig = MirrorUtils.getConfigFromConf(conf);
		taskId = context.getTaskAttemptID().getTaskID().toString();

		stateManager = StateManagerFactory.getStateManager(conf, dcmConfig);
		transferStatus = stateManager.getTransferStatus(taskId);
		read = false;

		fSplit = (MirrorInputSplit) split;
		inputs = fSplit.getSplits();
		System.out.println("Initializing transfer of : " + inputs.size()
				+ " files, with a total Size of : " + fSplit.getLength());

		inCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSourceConfig()
				.getConnectionConfig());

		outCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSinkConfig()
				.getConnectionConfig());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (!read) {
			digest = null;
			current = inputs.get(index);
			srcPath = MirrorUtils.getSimplePath(new Path(current.fileName));
			status = checkTransferStatus();

			if (status == null) {

				status = new TransferStatus();
				status.setStatus(Status.NEW);

				status.setInputSize(current.size);
				status.setTs(current.ts);
				status.setTaskID(taskId);
				System.out.println("Transfering file: " + current.fileName
						+ ", with size: " + current.size);

				try {
					analyzeStrategy();

					initializeStreams();

					digest = MirrorUtils.copy(in, out, context);

					status.setStatus(Status.COMPLETED);
					status.setMd5Digest(digest.getDigest());
					status.setOutputSize(digest.getByteCount());
				} catch (Exception e) {

					System.err.println("Error processing input: "
							+ e.getMessage());
					e.printStackTrace();
					status.setStatus(Status.FAILED);
					if (!dcmConfig.isIgnoreException()) {
						throw new IOException(e);
					}
				}
				closeStreams();

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

	private TransferStatus checkTransferStatus() {

		TransferStatus stat = transferStatus.get(srcPath);
		if (stat != null && stat.getStatus() == Status.COMPLETED) {
			return stat;
		}
		return null;
	}

	private void analyzeStrategy() {

		String srcCodec = null;

		if (dcmConfig.getSourceConfig().isTransformSource())
			srcCodec = MirrorUtils.getCodecNameFromPath(conf, srcPath);

		if (srcCodec != null) {
			status.setInputCompressed(false);

			if (dcmConfig.getSinkConfig().isUseCompression()) {
				if (srcCodec.equalsIgnoreCase(dcmConfig.getSinkConfig()
						.getCompressionCodec())) {
					status.setInputTransformed(false);
					status.setOutputCompressed(false);
				} else {
					status.setInputTransformed(true);
					status.setOutputCompressed(true);
				}
			} else {
				status.setInputTransformed(true);
				status.setOutputCompressed(false);
			}
		} else {
			if (dcmConfig.getSinkConfig().isUseCompression()) {
				status.setInputTransformed(false);
				status.setOutputCompressed(true);
			} else {
				status.setInputTransformed(false);
				status.setOutputCompressed(false);
			}
		}
		try {
			if (fSplit.getLength() < dcmConfig.getSourceConfig()
					.getCompressionThreshold()) {
				status.setInputTransformed(false);
				status.setOutputCompressed(false);
			}
		} catch (Exception e) {
			System.err.println("Error Analyzing Transfer strategy: "
					+ e.getMessage());
		}
	}

	private void initializeStreams() throws IOException,
			FileAlreadyExistsException {

		String destPath = dcmConfig.getSinkConfig().getPath() + srcPath;

		status.setInputPath(srcPath);
		status.setOutputPath(destPath);

		if (status.isInputTransformed()) {
			destPath = MirrorUtils.stripExtension(destPath);
			status.setOutputPath(destPath);
			
			in = inCodec.createInputStream(conf, srcPath);
			in = MirrorUtils.getCodecInputStream(conf, dcmConfig, srcPath, in);
		} else {
			in = inCodec.createInputStream(conf, srcPath);
		}

		if (status.isOutputCompressed()) {
			destPath = destPath + "."
					+ dcmConfig.getSinkConfig().getCompressionCodec();
			status.setOutputPath(destPath);
			
			if (!dcmConfig.getSinkConfig().isOverwriteFiles()) {
				if (outCodec.isExistsPath(destPath)) {
					throw new FileAlreadyExistsException(destPath);
				}
			}
			
			out = outCodec.createOutputStream(conf, destPath, dcmConfig
					.getSinkConfig().isAppend());
			out = MirrorUtils.getCodecOutputStream(conf, dcmConfig, destPath,
					out);
		} else {
			out = outCodec.createOutputStream(conf, destPath, dcmConfig
					.getSinkConfig().isAppend());
		}

		String statusMesg = "Processing: " + srcPath + " -> " + destPath;
		context.setStatus(statusMesg);
		System.out.println("Status: " + statusMesg);
	}

	private void closeStreams() {

		IOUtils.closeStream(in);
		IOUtils.closeStream(out);

		if (status.getStatus() == Status.COMPLETED
				&& dcmConfig.getSourceConfig().isDeleteSource()) {
			try {
				inCodec.deleteSoureFile(srcPath);
			} catch (Exception e) {
				System.err.println("Failed Deleting file: " + srcPath
						+ ", Exception: " + e.getMessage());
			}
		}
	}

	private void updateStatus() throws IOException {

		key.set(srcPath.toString());
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
	public void close() throws IOException {

		System.out.println("Transfer Complete...");
		IOUtils.closeStream(inCodec);
		IOUtils.closeStream(outCodec);
	}
}