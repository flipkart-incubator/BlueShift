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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by raj velu
 */
public class FstatComparisionDriver extends Configured implements Tool {

	private HashMap<String, Long> srcMap = null;
	private HashMap<String, Long> destMap = null;

	public FstatComparisionDriver() throws IOException {

		srcMap = new HashMap<String, Long>();
		destMap = new HashMap<String, Long>();
	}

	public int run(String[] args) throws Exception {

		String srcReport = args[0];
		String destReport = args[1];
		String destBasePath = args[2].trim().length() <= 1 ? null : args[2]
				.trim();

		loadReportFile(srcReport, null, srcMap);
		loadReportFile(destReport, destBasePath, destMap);

		generateDiff(args[3]);
		return 0;
	}

	private void generateDiff(String file) throws IOException {

		System.out.println("Total source records: " + srcMap.size());
		System.out.println("Total destination records: " + destMap.size());
		int scount = 0, mcount = 0, ccount = 0, ecount = 0;
		BufferedWriter success = new BufferedWriter(new FileWriter(file
				+ ".success"));
		BufferedWriter missing = new BufferedWriter(new FileWriter(file
				+ ".missing"));
		BufferedWriter changed = new BufferedWriter(new FileWriter(file
				+ ".changed"));
		BufferedWriter extra = new BufferedWriter(new FileWriter(file
				+ ".extra"));

		for (Entry<String, Long> entry : srcMap.entrySet()) {

			String path = entry.getKey();
			long size = entry.getValue();

			if (destMap.containsKey(path)) {
				if (destMap.get(path) != size) {
					changed.write(path);
					changed.newLine();
					ccount++;
				} else {
					success.write(path);
					success.newLine();
					scount++;
				}
			} else {
				missing.write(path);
				missing.newLine();
				mcount++;
			}
		}
		for (Entry<String, Long> entry : destMap.entrySet()) {

			String path = entry.getKey();

			if (!srcMap.containsKey(path)) {
				extra.write(path);
				extra.newLine();
				ecount++;
			}
		}
		success.close();
		missing.close();
		changed.close();
		extra.close();

		System.out.println("Total Successful Transfers: " + scount);
		System.out.println("Total Missing Transfers: " + mcount);
		System.out.println("Total Changed Transfers: " + ccount);
		System.out.println("Total Extra Transfers: " + ecount);
	}

	private void loadReportFile(String srcReport, String basepath,
			HashMap<String, Long> reportMap) throws FileNotFoundException,
			IOException {
		BufferedReader reader = new BufferedReader(new FileReader(srcReport));
		String line = null;

		while (null != (line = reader.readLine())) {

			line = line.trim();
			if (line.length() <= 1)
				continue;
			String[] kv = line.split(",");
			if (basepath != null) {
				kv[0] = kv[0].substring(basepath.length());
			}
			reportMap.put(kv[0], Long.parseLong(kv[1]));
		}
		reader.close();
	}

	private static void printUsageAndExit() {

		System.out.println("Snapshot : Usage: hadoop jar fstatcompare.jar "
				+ "<src_report> <dest_report> <dest_base_path> <report_file>");
		System.out
				.println("Example : hadoop jar fstatcompare.jar src_cluster.fstat dest_clust.fstat /projects diff.report");
		System.exit(1);
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 4) {
			printUsageAndExit();
		}
		int exitCode = ToolRunner.run(new FstatComparisionDriver(), args);
		System.exit(exitCode);
	}
}
