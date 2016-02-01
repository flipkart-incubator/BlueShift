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

package com.flipkart.fdp.migration.distcp.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileStatsDriver extends Configured implements Tool {

	private Configuration configuration;
	private FileSystem fs = null;
	private List<String> filePaths = null;
	private long startTS = 0;
	private long endTS = 0;

	public FileStatsDriver() throws IOException {

		configuration = getConf();
		if (configuration == null) {
			configuration = new Configuration();
		}
		fs = FileSystem.get(configuration);
	}

	public int run(String[] args) throws Exception {

		String path = args[0];
		startTS = Long.parseLong(args[1]);
		endTS = Long.parseLong(args[2]);

		if (startTS <= 0)
			startTS = 0;
		if (endTS <= 0)
			endTS = Long.MAX_VALUE;

		System.out.println("Starting Filestat generator, for Path: " + path
				+ ", Start TS: " + startTS + ", End TS: " + endTS);

		filePaths = getAllPaths(path);

		if(args.length > 3) {
			BufferedWriter writer = new BufferedWriter(new FileWriter(args[3]));
			for (String file : filePaths) {
				writer.write(file);
				writer.newLine();
			}
			writer.close();
		}
		return 0;
	}

	public List<String> getAllPaths(String path) throws Exception {
		return getAllPaths(Arrays.asList(path.split(",")));
	}

	public List<String> getAllPaths(Collection<String> paths) throws Exception {

		List<String> inputLocs = new ArrayList<String>();
		for (String loc : paths) {
			System.out.println("Scanning path for Files: " + loc);
			loc = loc.trim();
			inputLocs.addAll(getAllFilePath(new Path(loc)));
		}
		System.out.println("Total Input Paths : " + inputLocs.size());

		return inputLocs;
	}

	public List<String> getAllFilePath(Path filePath) throws IOException {
		List<String> fileList = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(filePath);
		for (FileStatus fileStat : fileStatus) {
			if (fileStat.isDirectory()) {
				fileList.addAll(getAllFilePath(fileStat.getPath()));
			} else {
				long ts = fileStat.getModificationTime();
				if (ts >= startTS && ts <= endTS)
					fileList.add(fileStat.getPath().toUri().getPath() + ","
							+ fileStat.getLen());
			}
		}
		return fileList;
	}

	private static void printUsageAndExit() {

		System.out.println("Snapshot : Usage: yarn jar <jar location> "
				+ "<basepath> <startts> <endts> <local reportpath>");
		System.out
				.println("Example : yarn jar <jar location> /projects 0 0 projects.fstat");
		System.exit(1);
	}

	public static void main(String args[]) throws Exception {

		if (args.length < 3) {
			printUsageAndExit();
		}
		int exitCode = ToolRunner.run(new FileStatsDriver(), args);
		System.exit(exitCode);
	}
}
