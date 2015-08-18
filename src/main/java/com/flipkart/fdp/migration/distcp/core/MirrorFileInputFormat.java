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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.flipkart.fdp.migration.db.DBInitializer;
import com.flipkart.fdp.migration.db.api.CMapperDetailsApi;
import com.flipkart.fdp.migration.db.models.MapperDetails;
import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory.Optimizer;

public class MirrorFileInputFormat extends InputFormat<Text, Text> {

	public static final String DCM_CONFIG = "dcm_config";
	public static final String INCLUDE_FILES = "include_files";
	public static final String EXCLUDE_FILES = "exclude_files";

	private DCMCodec dcmCodec = null;
	private Configuration conf = null;
	private DCMConfig dcmConfig = null;
	private Set<String> excludeList = null;
	private Set<String> includeList = null;
	private CMapperDetailsApi mapDetails = null;
	private DBInitializer dbHelper = null;

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		conf = context.getConfiguration();
		dcmConfig = MirrorUtils.getConfigFromConf(conf);

		dcmCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSourceConfig()
				.getConnectionConfig());

		dbHelper = new DBInitializer(dcmConfig.getDbConfig());
		mapDetails = new CMapperDetailsApi(dbHelper);

		excludeList = MirrorUtils.getStringAsLists(conf.get(EXCLUDE_FILES));
		includeList = MirrorUtils.getStringAsLists(conf.get(INCLUDE_FILES));

		List<InputSplit> splits = new ArrayList<InputSplit>();
		Set<OptimTuple> locations = new HashSet<OptimTuple>();

		long totalBatchSize = 0;
		try {
			List<FileStatus> fstats = null;
			if (includeList != null && includeList.size() > 0)
				fstats = dcmCodec.getInputPaths(includeList);
			else
				fstats = dcmCodec.getInputPaths(dcmConfig.getSourceConfig()
						.getPath());

			processPreviousMaps();

			for (FileStatus fstat : fstats) {

				if (!ignoreFile(fstat)) {
					locations.add(new OptimTuple(fstat.getPath().toString(),
							fstat.getLen()));
				}
			}
			int numWorkers = locations.size();
			if (dcmConfig.getNumWorkers() > 0
					&& dcmConfig.getNumWorkers() < numWorkers) {
				numWorkers = dcmConfig.getNumWorkers();
				List<Set<IInputJob>> splitTasks = optimizeWorkload(locations,
						numWorkers);
				for (Set<IInputJob> stats : splitTasks) {
					List<FileTuple> tuple = new ArrayList<FileTuple>();
					long size = 0;
					for (IInputJob stat : stats) {
						tuple.add(new FileTuple(stat.getJobKey(), stat
								.getJobSize()));
						size += stat.getJobSize();
					}
					totalBatchSize += size;
					splits.add(new MirrorInputSplit(tuple, size));
				}
			} else {
				for (OptimTuple stat : locations) {
					List<FileTuple> tuple = new ArrayList<FileTuple>();
					tuple.add(new FileTuple(stat.jobKey, stat.size));
					splits.add(new MirrorInputSplit(tuple, stat.getJobSize()));
					totalBatchSize += stat.getJobSize();
				}
			}
			if (splits.size() <= 0)
				throw new Exception("No Inputs Identified for Processing.. ");
			sortSplits(splits);
			System.out.println("Total input paths to process: "
					+ locations.size() + ", Total input splits: "
					+ splits.size());
			System.out.println("Total Data to Transfer: " + totalBatchSize);
		} catch (Exception e) {
			throw new IOException(e);
		}

		System.out.println("Finished getSplits");
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

	private void processPreviousMaps() throws Exception {
		List<MapperDetails> maps = mapDetails.getAllMapperDetails(dcmConfig
				.getBatchID());
		if (maps == null || maps.size() <= 0)
			return;
		for (MapperDetails map : maps) {
			if (map.getStatus() == Status.COMPLETED)
				excludeList.add(map.getFilePath());
		}
	}

	private boolean ignoreFile(FileStatus fileStat) {

		boolean ignoreFile = false;

		// File Size Based Rules
		long fileSize = fileStat.getLen();
		if (fileSize <= 0 && dcmConfig.getSourceConfig().isIgnoreEmptyFiles())
			ignoreFile |= true;

		if (fileSize < dcmConfig.getSourceConfig().getMinFilesize())
			ignoreFile |= true;

		if (dcmConfig.getSourceConfig().getMaxFilesize() != -1
				&& fileSize > dcmConfig.getSourceConfig().getMaxFilesize())
			ignoreFile |= true;

		// File Time Based rules
		long ts = dcmConfig.getSourceConfig().getStartTS();
		if (ts > 0 && fileStat.getModificationTime() < ts)
			ignoreFile |= true;

		ts = dcmConfig.getSourceConfig().getEndTS();
		if (ts > 0 && fileStat.getModificationTime() > ts)
			ignoreFile |= true;

		// File Name based rules
		String path = MirrorUtils.getSimplePath(fileStat.getPath());
		if (excludeList.contains(path))
			ignoreFile |= true;

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

	public List<Set<IInputJob>> optimizeWorkload(Set<OptimTuple> tasks,
			int numMappers) {

		System.out.println("Total Tasks: " + tasks.size() + " Total Reducers: "
				+ numMappers);
		Optimizer optimizer = Optimizer.PRIORITY_QUEUE_BASED;

		System.out.println("Using Optimizer: " + optimizer.toString());
		IJobLoadOptimizer iJobLoadOptimizer = JobLoadOptimizerFactory
				.getJobLoadOptimizerFactory(optimizer);
		List<Set<IInputJob>> optimizedLoadSets = iJobLoadOptimizer
				.getOptimizedLoadSets(tasks, numMappers);
		return optimizedLoadSets;
	}

	public static class MirrorFileRecordReader extends RecordReader<Text, Text> {

		private boolean read = false;

		private String srcPath = null;

		private Text key = new Text();
		private Text value = new Text();

		private MirrorInputSplit fSplit = null;

		private DCMConfig dcmConfig = null;
		private Configuration conf = null;

		private DBInitializer dbHelper = null;
		private CMapperDetailsApi mapDetails = null;
		private String taskID = null;
		private List<FileTuple> inputs = null;
		private int index = 0;
		private FileTuple current = null;

		private InputStream in = null;
		private OutputStream out = null;

		private DCMCodec inCodec = null;
		private DCMCodec outCodec = null;

		private boolean failed = false;

		boolean useCompression = false;
		boolean transform = false;

		private String digest = null;

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

			dbHelper = new DBInitializer(dcmConfig.getDbConfig());
			mapDetails = new CMapperDetailsApi(dbHelper);

			read = false;

			fSplit = (MirrorInputSplit) split;
			inputs = fSplit.getSplits();
			System.out.println("Initializing transfer of : " + inputs.size()
					+ " files, with a total Size of : " + fSplit.getLength());

			taskID = context.getTaskAttemptID().toString();

			inCodec = DCMCodecFactory.getCodec(conf, dcmConfig
					.getSourceConfig().getConnectionConfig());

			outCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSinkConfig()
					.getConnectionConfig());
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (!read) {

				failed = false;
				current = inputs.get(index);
				srcPath = new Path(current.fileName).toUri().getPath()
						.toString();
				System.out.println("Transfering file: " + current.fileName
						+ ", with size: " + current.size);
				try {

					analyzeStrategy();

					initializeStreams();

					key.set(srcPath.toString());

					digest = MirrorUtils.copy(in, out, context);

					value.set("Success: " + digest);
				} catch (Exception e) {
					e.printStackTrace();
					failed = true;
					if (!dcmConfig.isIgnoreException()) {
						throw new IOException(e);
					} else {
						value.set("Failed: " + e.getMessage());
					}
				}

				closeStreams();

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

		private void analyzeStrategy() {

			String srcCodec = MirrorUtils.getCodecNameFromPath(conf, srcPath);

			if (srcCodec != null) {
				if (dcmConfig.getSinkConfig().isUseCompression()) {
					if (srcCodec.equalsIgnoreCase(dcmConfig.getSinkConfig()
							.getCompressionCodec())) {
						transform = false;
						useCompression = false;
					} else {
						transform = true;
						useCompression = true;
					}
				} else {
					transform = true;
					useCompression = false;
				}
			} else {
				if (dcmConfig.getSinkConfig().isUseCompression()) {
					transform = false;
					useCompression = true;
				} else {
					transform = false;
					useCompression = false;
				}
			}
			try {
				if (fSplit.getLength() < dcmConfig.getSourceConfig()
						.getCompressionThreshold()) {
					transform = false;
					useCompression = false;
				}
			} catch (Exception e) {
				// ignore exception
			}
		}

		private void initializeStreams() throws IOException {

			String destPath = dcmConfig.getSinkConfig().getPath() + srcPath;

			if (transform) {
				in = inCodec.createInputStream(conf, srcPath);
				in = MirrorUtils.getCodecInputStream(conf, dcmConfig, srcPath,
						in);
				destPath = MirrorUtils.stripExtension(destPath);
			} else {
				in = inCodec.createInputStream(conf, srcPath);
			}

			if (useCompression) {
				destPath = destPath + "."
						+ dcmConfig.getSinkConfig().getCompressionCodec();
				out = outCodec.createOutputStream(conf, destPath, dcmConfig
						.getSinkConfig().isAppend());
				out = MirrorUtils.getCodecOutputStream(conf, dcmConfig,
						destPath, out);
			} else {
				out = outCodec.createOutputStream(conf, destPath, dcmConfig
						.getSinkConfig().isAppend());
			}
			String status = "Processing: " + srcPath + " -> " + destPath;
			context.setStatus(status);
			System.out.println("Status: " + status);
		}

		private void closeStreams() {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}

		private void updateStatus() throws IOException {

			String id = taskID + "_" + index;
			try {
				mapDetails.createMapperDetails(dcmConfig.getBatchID(), id,
						srcPath, digest, failed ? Status.FAILED
								: Status.COMPLETED);
			} catch (Exception e) {
				System.out.println("Caught exception persisting state to DB..."
						+ e.getMessage());
				try {
					mapDetails = new CMapperDetailsApi(dbHelper);
					mapDetails.createMapperDetails(dcmConfig.getBatchID(), id,
							srcPath, digest, failed ? Status.FAILED
									: Status.COMPLETED);
				} catch (Exception ei) {
					System.out
							.println("Caught exception re-persisting state to DB..."
									+ e.getMessage());
					e.printStackTrace();
					if (failed && !dcmConfig.isIgnoreException()) {
						throw new IOException(ei);
					}
				}
			}
		}

		@Override
		public void close() throws IOException {

			System.out.println("Transfer Complete...");
			IOUtils.closeStream(inCodec);
			IOUtils.closeStream(outCodec);
		}
	}
}
