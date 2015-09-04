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

package com.flipkart.fdp.migration.distcp.state;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;
import com.google.gson.Gson;

public class HDFSStateManager implements StateManager {

	public static final String STATUS_PATH_KEY = "status_path";
	public static final String RUN_PATH_KEY = "run_path";
	public static final String STATUS_PATH = "status";
	public static final String REPORT_PATH = "report";
	public static final String CONFIG_FILE_NAME = "config.json";
	public static final String LOCK_FILE_NAME = ".lock";
	public static final String PREVIOUS_STATE_FILE_NAME = "prev.state";
	public static final String RUN_ID = "run_id";

	private Configuration configuration = null;
	private DCMConfig dcmConfig = null;
	private FileSystem fs = null;

	private BufferedWriter statusWriter = null;

	private Path runPath = null;
	private Path statusPath = null;
	private Path lockFilePath = null;
	private Path batchBasePath = null;
	private String runId = null;

	public HDFSStateManager(Configuration conf, DCMConfig config)
			throws IOException {

		System.out.println("Instantiating HDFS based Statemanager...");
		this.configuration = conf;
		this.dcmConfig = config;

		batchBasePath = new Path(dcmConfig.getStatusPath() + "/"
				+ dcmConfig.getBatchName());

		if (dcmConfig.getStatusPath().startsWith("file://")) {
			// local state manager
			fs = FileSystem.get(batchBasePath.toUri(), configuration);
		} else {
			// hdfs state manager
			fs = FileSystem.get(configuration);
		}

		runId = configuration.get(RUN_ID);

		if (runId == null) {
			runId = String.valueOf(System.currentTimeMillis());
			runPath = new Path(batchBasePath, runId);
			statusPath = new Path(runPath, STATUS_PATH);

			configuration.set(RUN_ID, runId);
			configuration.set(STATUS_PATH_KEY, statusPath.toString());
			configuration.set(RUN_PATH_KEY, runPath.toString());
			saveConfig();
			System.out.println("New HDFS Run Location: " + runPath.toString());
		} else {
			runPath = new Path(configuration.get(RUN_PATH_KEY));
			statusPath = new Path(configuration.get(STATUS_PATH_KEY));
		}
		lockFilePath = new Path(batchBasePath, LOCK_FILE_NAME);

	}

	public void lockBatch() throws IOException {
		OutputStream out = fs.create(lockFilePath, false);
		out.close();
	}

	public void unLockBatch() throws IOException {
		fs.delete(lockFilePath, false);
	}

	public boolean isBatchLocked() throws IOException {

		return fs.exists(lockFilePath);
	}

	public void beginBatch() throws IOException {

		if (isBatchLocked()) {
			System.out.println("Batch Lock Exists: " + lockFilePath);
			throw new IOException(
					"Batch is locked, Manually release the lock or Please ensure no other execution is happening on this batch...");
		}
		fs.mkdirs(batchBasePath);
		fs.mkdirs(statusPath);
		lockBatch();
	}

	public void completeBatch(Status status) throws IOException {
		unLockBatch();
		FSDataOutputStream out = fs.create(new Path(statusPath, status
				.toString()));
		out.close();
	}

	public Path getStatusOutputPath() {
		return statusPath;
	}

	public void updateTransferStatus(TransferStatus status) throws IOException {

		if (statusWriter == null) {
			Path statPath = new Path(statusPath, status.getTaskID());

			statusWriter = new BufferedWriter(new OutputStreamWriter(
					fs.create(statPath)));

		}
		statusWriter.write(status.toString());
		statusWriter.newLine();
		statusWriter.flush();
	}

	public void saveConfig() throws IOException {
		FSDataOutputStream out = fs.create(new Path(runPath, CONFIG_FILE_NAME));
		out.writeBytes(dcmConfig.toString());
		out.close();
	}

	public void savePreiviousTransferStatus(
			Map<String, TransferStatus> prevState) throws IOException {

		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				fs.create(new Path(runPath, PREVIOUS_STATE_FILE_NAME))));

		for (TransferStatus status : prevState.values()) {
			out.write(status.toString());
			out.newLine();
		}
		out.close();
	}

	public Map<String, TransferStatus> getPreviousTransferStatus()
			throws IOException {

		Map<String, TransferStatus> status = new HashMap<String, TransferStatus>();
		FileStatus fstats[] = null;

		try {
			fstats = fs.listStatus(batchBasePath);
		} catch (Exception e) {
			System.out.println("No Previous states found: " + e.getMessage());
		}

		if (fstats == null || fstats.length <= 0)
			return status;

		List<FileStatus> fstatList = new ArrayList<FileStatus>();

		for (FileStatus fstat : fstats) {
			if (fstat.isDirectory())
				fstatList.add(fstat);
		}

		Collections.sort(fstatList, new Comparator<FileStatus>() {

			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				return (int) (o2.getModificationTime() - o1
						.getModificationTime());
				// decending order sort by timestamp
			}
		});

		// ignore the current state folder as well.
		fstatList.remove(0);

		for (FileStatus fstat : fstatList) {
			System.out.println("Processing State History: " + fstat.getPath());

			Path spath = new Path(fstat.getPath(), PREVIOUS_STATE_FILE_NAME);
			List<TransferStatus> stats = getAllStats(new Path(fstat.getPath(),
					STATUS_PATH));
			mergeStates(status, stats);
			if (fs.exists(spath)) {
				stats = getAllStats(spath);
				mergeStates(status, stats);
				break;
			}
		}
		return status;
	}

	private void mergeStates(Map<String, TransferStatus> status,
			List<TransferStatus> stats) {

		if (stats != null && stats.size() > 0) {
			for (TransferStatus stat : stats) {
				TransferStatus ostat = status.get(stat.getInputPath());
				if (ostat == null) {
					status.put(stat.getInputPath(), stat);
				}
				// else {
				// status.put(stat.getInputPath(), stat);
				// }
			}
		}
	}

	@Override
	public Map<String, TransferStatus> getTransferStatus(String taskId)
			throws IOException {

		List<TransferStatus> stats = getAllStats(new Path(statusPath, taskId));
		Map<String, TransferStatus> status = new HashMap<String, TransferStatus>();
		if (stats != null && stats.size() > 0) {

			for (TransferStatus stat : stats) {
				status.put(stat.getInputPath(), stat);
			}
		}
		return status;
	}

	private List<TransferStatus> getAllStats(Path path) throws IOException {

		Gson gson = new Gson();
		List<TransferStatus> status = new ArrayList<TransferStatus>();

		FileStatus fstats[] = null;
		if (fs.isDirectory(path)) {

			fstats = fs.listStatus(path);
		} else {
			try {
				fstats = new FileStatus[1];
				fstats[0] = fs.getFileStatus(path);
			} catch (Exception e) {
				return status;
			}
		}
		if (fstats == null || fstats.length <= 0)
			return status;

		for (FileStatus fstat : fstats) {

			if (fstat.isFile()) {
				try {

					BufferedReader reader = new BufferedReader(
							new InputStreamReader(fs.open(fstat.getPath())));
					String line = null;
					while (null != (line = reader.readLine())) {
						if (line.trim().length() <= 1)
							continue;
						try {
							TransferStatus tstat = gson.fromJson(line,
									TransferStatus.class);
							if (tstat != null)
								status.add(tstat);
						} catch (Exception ein) {
							System.out
									.println("Exception Reading from location: "
											+ fstat.getPath()
											+ ", Message: "
											+ ein.getMessage());
						}
					}
					reader.close();
				} catch (Exception e) {
					System.out.println("Exception reading previous state: "
							+ e.getMessage());
				}
			}
		}
		return status;
	}

	@Override
	public Path getReportPath() {

		return new Path(runPath, REPORT_PATH);
	}

	@Override
	public String getRunId() {
		return runId;
	}

	@Override
	public void close() throws IOException {
		if (statusWriter != null) {
			IOUtils.closeStream(statusWriter);
			statusWriter = null;
		}
	}

}
