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
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;

public class MirrorFileRecordReader extends RecordReader<Text, Text> {

	private String srcPath = null;
	private int progressCount = 0;

	private MirrorInputSplit fSplit = null;

	private DCMConfig dcmConfig = null;
	private Configuration conf = null;

	private List<MirrorDCMImpl.FileTuple> inputs = null;
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

	private MirrorDCMImpl.FileTuple current = null;

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
			progressCount++;
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

	private void initializeStreams() throws IOException {

		String basePath = dcmConfig.getSinkConfig().getPath();
		if (basePath != null && basePath.trim().length() > 1) {
			basePath = dcmConfig.getSinkConfig().getPath().trim() + srcPath;
		} else {
			basePath = srcPath;
		}

		String destPath = basePath;

		status.setInputPath(srcPath);
		status.setOutputPath(destPath);

		if (status.isInputTransformed()) {
			destPath = MirrorUtils.stripExtension(destPath);
			status.setOutputPath(destPath);

			in = inCodec.createInputStream(srcPath);
			in = MirrorUtils.getCodecInputStream(conf, dcmConfig, srcPath, in);
		} else {
			in = inCodec.createInputStream(srcPath);
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

			out = MirrorUtils.getCodecOutputStream(conf, dcmConfig, destPath,
					out);
		} else {
			if (!dcmConfig.getSinkConfig().isOverwriteFiles()) {
				if (outCodec.isExistsPath(destPath)) {
					throw new FileAlreadyExistsException(destPath);
				}
			}
			out = outCodec.createOutputStream(destPath, dcmConfig
					.getSinkConfig().isAppend());
		}

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

		key.set(status.getInputPath());
		value.set(status.toString());

		if (status.getStatus() == Status.COMPLETED) {
			context.getCounter(MirrorDCMImpl.BLUESHIFT_COUNTER.SUCCESS_COUNT)
					.increment(1);
		} else {
			context.getCounter(MirrorDCMImpl.BLUESHIFT_COUNTER.FAILED_COUNT)
					.increment(1);
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
		return read ? 1 : (((float) progressCount / inputs.size()));
	}

	@Override
	public void close() throws IOException {
		
		System.out.println("Transfer Complete...");
		IOUtils.closeStream(inCodec);
		IOUtils.closeStream(outCodec);
		IOUtils.closeStream(stateManager);
	}

	public void closeStreams() throws IOException {

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

	private void analyzeStrategy() {

		String srcCodec = null;

		if (dcmConfig.getSourceConfig().isTransformSource())
			srcCodec = MirrorUtils.getCodecNameFromPath(conf, srcPath);

		if (srcCodec != null)
			status.setInputCompressed(false);

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

}