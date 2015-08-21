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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.BLUESHIFT_COUNTER;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.MirrorMapper;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.MirrorReducer;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.google.gson.Gson;

/**
 * Created by raj velu
 */
public class MirrorDistCPDriver extends Configured implements Tool {

	private Configuration configuration;

	private DCMConfig dcmConfig = null;
	private Set<String> includeList = null;
	private Set<String> excludeList = null;

	private StateManager stateManager = null;

	public MirrorDistCPDriver(DCMConfig config) throws IOException {

		this.dcmConfig = config;
		formatSourceConfig();
	}

	private void formatSourceConfig() {
		includeList = MirrorUtils.getFileAsLists(dcmConfig.getSourceConfig()
				.getIncludeListFile());
		excludeList = MirrorUtils.getFileAsLists(dcmConfig.getSourceConfig()
				.getExcludeListFile());
	}

	public int run(String[] args) throws Exception {

		configuration = getConf();

		// Setting task timeout to 2 hrs
		configuration.setLong("mapred.task.timeout", 1000 * 60 * 60 * 2);

		populateConfFromDCMConfig();

		int jobReturnValue = 0;
		stateManager = StateManagerFactory.getStateManager(configuration, dcmConfig);

		try {
			stateManager.beginBatch();
		} catch (Exception e) {
			System.out.println("Exception starting batch: " + e.getMessage());
			e.printStackTrace();
			return 1;
		}

		try {

			Job job = createJob(configuration);

			System.out.println("Launching Job - Blueshift v 0.1 - "
					+ dcmConfig.getBatchName());
			jobReturnValue = job.waitForCompletion(true) ? 0 : 1;

			System.out.println("Job Complete...");

			processJobCounters(job);
		} catch (Throwable t) {
			jobReturnValue = 1;
			System.out.println("Job Failed...");
			t.printStackTrace();
		}
		stateManager.completeBatch(jobReturnValue != 0 ? Status.FAILED
				: Status.COMPLETED);

		return jobReturnValue;

	}

	private void processJobCounters(Job job) {
		try {
			Counters counters = job.getCounters();

			long failedCount = counters.findCounter(
					BLUESHIFT_COUNTER.FAILED_COUNT).getValue();

			long successCount = counters.findCounter(
					BLUESHIFT_COUNTER.SUCCESS_COUNT).getValue();

			System.out.println("Total Success Transfers: " + successCount
					+ ", Total Failed Transfers: " + failedCount);
			if (failedCount > 0) {
				System.err.println("There are " + failedCount
						+ " transfers, Please re-run the job...");
			}
		} catch (Exception e) {
			System.out.println("Error processing job counters: "
					+ e.getMessage());
		}
	}

	private Job createJob(Configuration configuration) throws Exception {

		System.out.println("Initializing Blueshift v 0.1...");
		System.out.println("Configuration: " + dcmConfig.toString());
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration, "Blueshift v 0.1 - "
				+ dcmConfig.getBatchName());

		job.setJarByClass(MirrorDistCPDriver.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MirrorMapper.class);
		job.setReducerClass(MirrorReducer.class);

		job.setInputFormatClass(MirrorFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		MirrorFileInputFormat.setExclusionsFileList(configuration, excludeList);
		MirrorFileInputFormat.setInclusionFileList(configuration, includeList);

		String statusPath = dcmConfig.getStatusPath() + "/"
				+ dcmConfig.getBatchName() + "_" + System.currentTimeMillis();

		FileOutputFormat.setOutputPath(job, new Path(statusPath));

		job.setNumReduceTasks(1);

		System.out
				.println("Job Initialization Complete, The status of the Mirror job will be written to: "
						+ statusPath);
		return job;
	}

	private void populateConfFromDCMConfig() {

		configuration.set(MirrorFileInputFormat.DCM_CONFIG,
				dcmConfig.toString());
	}

	@SuppressWarnings("static-access")
	public static DCMConfig getParams(String[] args) throws Exception {

		Options options = new Options();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		options.addOption(OptionBuilder.withArgName("property=value")
				.hasArgs(2).withValueSeparator()
				.withDescription("use value for given property").create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		String path = null;
		if (cmd.hasOption('p')) {
			path = cmd.getOptionValue('p');
		} else if (cmd.hasOption('P')) {
			path = cmd.getOptionValue('P');
		}
		if (path == null || !new File(path).exists()) {
			throw new Exception("Unable to load Config File...");
		}
		Gson gson = new Gson();
		return gson.fromJson(new FileReader(path), DCMConfig.class);
	}

	private static void printUsageAndExit() {

		System.out
				.println("Snapshot : Usage: hadoop jar blushift.jar -P<config.json> "
						+ "-P<config file Name>");
		System.out.println("Example : hadoop jar blushift.jar -Pdriver.json");
		System.exit(1);
	}

	public static void main(String args[]) throws Exception {

		if (args.length < 1) {
			printUsageAndExit();
		}
		int exitCode = ToolRunner.run(new MirrorDistCPDriver(getParams(args)),
				args);
		System.exit(exitCode);
	}
}
