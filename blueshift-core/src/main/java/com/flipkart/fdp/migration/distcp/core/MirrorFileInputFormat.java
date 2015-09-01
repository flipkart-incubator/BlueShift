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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.flipkart.fdp.optimizer.OptimTuple;

public class MirrorFileInputFormat extends InputFormat<Text, Text> {

	public static final String DCM_CONFIG = "dcm_config";
	public static final String INCLUDE_FILES = "include_files";
	public static final String EXCLUDE_FILES = "exclude_files";

	private DCMCodec dcmCodec = null;
	private Configuration conf = null;
	private DCMConfig dcmConfig = null;
	private StateManager stateManager = null;

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		Set<String> excludeList = null;
		Set<String> includeList = null;
		Map<String, TransferStatus> previousState = null;

		System.out.println("Calculating Job Splits...");
		conf = context.getConfiguration();
		dcmConfig = MirrorUtils.getConfigFromConf(conf);

		dcmCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSourceConfig()
				.getConnectionConfig(), null);

		excludeList = getExclusionsFileList(conf);
		includeList = getInclusionFileList(conf);
		HashMap<String, FileTuple> inputFileMap = new HashMap<String, FileTuple>();

		List<InputSplit> splits = new ArrayList<InputSplit>();
		Set<OptimTuple> locations = new HashSet<OptimTuple>();

		long totalBatchSize = 0;
		try {
			System.out.println("Scanning source location...");
			List<FileTuple> fstats = null;
			if (includeList != null && includeList.size() > 0)
				fstats = dcmCodec.getInputPaths(includeList, excludeList);
			else
				fstats = dcmCodec.getInputPaths(dcmConfig.getSourceConfig()
						.getPath(), excludeList);

			stateManager = StateManagerFactory.getStateManager(conf, dcmConfig);

			System.out
					.println("Fetching previous transfer states from StateManager...");
			previousState = stateManager.getPreviousTransferStatus();

			System.out
					.println("Filtering Input File Set based on User defined filters.");
			for (FileTuple fstat : fstats) {

				if (!ignoreFile(fstat, excludeList, previousState)) {

					locations.add(new OptimTuple(fstat.fileName, fstat.size));
					inputFileMap.put(fstat.fileName, fstat);
				}
			}
			System.out.println("Optimizing Splits...");

			splits.addAll(MirrorUtils.optimizeInputSplits(conf, dcmConfig,
					locations, inputFileMap));

			if (splits.size() <= 0)
				throw new Exception("No Inputs Identified for Processing.. ");

			sortSplits(splits);
			System.out.println("Total input paths to process: "
					+ locations.size() + ", Total input splits: "
					+ splits.size());
			System.out.println("Total Data to Transfer: " + totalBatchSize);

			stateManager.savePreiviousTransferStatus(previousState);
		} catch (Exception e) {
			throw new IOException(e);
		}

		System.out.println("Done Calculating splits...");
		return splits;
	}

	private void sortSplits(List<InputSplit> splits) {
		Collections.sort(splits, new Comparator<InputSplit>() {
			// @Override
			public int compare(InputSplit f0, InputSplit f1) {
				try {
					if (f1.getLength() > f0.getLength())
						return 1;
					if (f1.getLength() < f0.getLength())
						return -1;
					return 0;
				} catch (Exception e) {
					return 0;
				}
			}
		});
	}

	private boolean ignoreFile(FileTuple fileStat, Set<String> excludeList,
			Map<String, TransferStatus> previousState) {

		boolean ignoreFile = false;

		// File Size Based Rules
		long fileSize = fileStat.size;
		if (fileSize <= 0 && dcmConfig.getSourceConfig().isIgnoreEmptyFiles())
			return true;

		if (fileSize < dcmConfig.getSourceConfig().getMinFilesize())
			return true;

		if (dcmConfig.getSourceConfig().getMaxFilesize() != -1
				&& fileSize > dcmConfig.getSourceConfig().getMaxFilesize())
			return true;

		// File Time Based rules
		long ts = dcmConfig.getSourceConfig().getStartTS();
		if (ts > 0 && fileStat.ts < ts)
			return true;

		ts = dcmConfig.getSourceConfig().getEndTS();
		if (ts > 0 && fileStat.ts > ts)
			return true;

		// File Name based rules
		String path = fileStat.fileName;
		if (excludeList.contains(path))
			return true;

		if (previousState.containsKey(path)) {
			TransferStatus details = previousState.get(path);
			if (details.getStatus() == Status.COMPLETED) {
				if (dcmConfig.getSourceConfig().isIncludeUpdatedFiles()
						&& details.getTs() < fileStat.ts)
					return false;

				return true;
			}
		}
		return ignoreFile;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new MirrorFileRecordReader();
	}

	public static void setExclusionsFileList(Configuration conf,
			Collection<String> files) {
		conf.set(MirrorFileInputFormat.EXCLUDE_FILES,
				MirrorUtils.getListAsString(files));

	}

	public static void setInclusionFileList(Configuration conf,
			Collection<String> files) {
		conf.set(MirrorFileInputFormat.INCLUDE_FILES,
				MirrorUtils.getListAsString(files));
	}

	public static Set<String> getExclusionsFileList(Configuration conf) {

		return MirrorUtils.getStringAsLists(conf.get(EXCLUDE_FILES));

	}

	public static Set<String> getInclusionFileList(Configuration conf) {
		return MirrorUtils.getStringAsLists(conf.get(INCLUDE_FILES));
	}

}
