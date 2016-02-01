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
import java.net.URI;
import java.net.URISyntaxException;
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

public class FileCountDriver extends Configured implements Tool {

	private Configuration configuration;
	private FileSystem destFS = null;
	private FileSystem srcFS = null;
	private List<String> destFilePaths = null;
	private List<String> srcFilePaths = null;

	public FileCountDriver() throws IOException {
		configuration = getConf();
		if (configuration == null) {
			configuration = new Configuration();
		}
	}

	public int run(String[] args) throws Exception {

		String srcPath = args[0];
		String destBasePath = args[1];
		String destPath = destBasePath + srcPath;
		String srcNN = args[2];
		String destNN = args[3];
		boolean reportEnabled = false;
		
		if(args.length > 5) {
			reportEnabled = true;
		}

		this.srcFS = getFileSystem(srcNN);
		this.destFS = getFileSystem(destNN);

		System.out.println("srcPath : " + srcPath);
		System.out.println("destPath : " + destPath);
		System.out.println("srcNN : " + srcNN);
		System.out.println("destNN : " + destNN);
		System.out.println("srcFS : " + this.srcFS);
		System.out.println("destFS : " + this.destFS);
		
		this.srcFilePaths = getAllPaths(srcPath, this.srcFS, "");
		this.destFilePaths = getAllPaths(destPath, this.destFS, destBasePath);
		
		compare(srcNN, destNN);
		
		if(reportEnabled) {
			writeReport(this.srcFilePaths, args[4]);
			writeReport(this.destFilePaths, args[5]);
		}
		return 0;
	}

	private void compare(String srcNN, String destNN) {
		if(destFilePaths.size() != srcFilePaths.size()) {
			System.out.println("File count not matching : src[" + srcFilePaths.size() + "], dest["
					+ destFilePaths.size() + "]");
		}
		
		System.out.println("Missing in dest cluster : [" + destNN +"]");
		for(String srcFileName : this.srcFilePaths) {
			if(!this.destFilePaths.contains(srcFileName)) {
				System.out.println(srcFileName);
			}
		}
		
		System.out.println("Missing in src cluster : [" + srcNN +"]");
		for(String destFileName : this.destFilePaths) {
			if(!this.srcFilePaths.contains(destFileName)) {
				System.out.println(destFileName);
			}
		}
	}

	private void writeReport(List<String> filePaths, String fileName) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
		for (String file : filePaths) {
			writer.write(file);
			writer.newLine();
		}
		writer.close();
	}

	private FileSystem getFileSystem(String nn) throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.newInstance(new URI(nn), configuration);
		return fileSystem;
	}

	public List<String> getAllPaths(String path, FileSystem fs, String destBasePath) throws Exception {
		System.out.println("Starting FileCount generator, for Path: " + path);
		return getAllPaths(Arrays.asList(path.split(",")), fs, destBasePath);
	}

	public List<String> getAllPaths(Collection<String> paths, FileSystem fs, String destBasePath) throws Exception {

		List<String> inputLocs = new ArrayList<String>();
		for (String loc : paths) {
			System.out.println("Scanning path for Files: " + loc);
			loc = loc.trim();
			inputLocs.addAll(getAllFilePath(new Path(loc), fs, destBasePath));
		}
		System.out.println("Total Input Paths : " + inputLocs.size());
		return inputLocs;
	}

	public List<String> getAllFilePath(Path filePath, FileSystem fs, String destBasePath) throws IOException {
		List<String> fileList = new ArrayList<String>();
		List<String> inputPaths = new ArrayList<String>();
		FileStatus[] fileStatus = fs.globStatus(filePath);
		for (FileStatus fileStat : fileStatus) {
			if (fileStat.isFile()) {
				fileList.add(trimExtension(fileStat.getPath().toUri().getPath(), destBasePath));
			} else {
				//System.out.println("Found a directory : " + fileStat.getPath().toUri().getPath());
				inputPaths.add(fileStat.getPath().toUri().getPath());
			}
		}
		
		System.out.println("InputPaths size : " + inputPaths.size());
		if (inputPaths.size() > 0) {
			for (String path : inputPaths) {
				List<String> fstat = getFileStatusRecursive(new Path(path), fs, destBasePath);
				fileList.addAll(fstat);
			}
		}
		
		return fileList;
	}
	
	private String trimExtension(String path, String destBasePath) {
		int startIndex = destBasePath.length();
		int lastIndex = path.lastIndexOf('.');
		if(lastIndex != -1 && (path.endsWith(".4mc") || path.endsWith(".snappy")
				|| path.endsWith(".gzip") || path.endsWith(".bzip2")) ) {
			return path.substring(startIndex, lastIndex);
		}
		return path;
	}
	

	public List<String> getFileStatusRecursive(Path path, FileSystem fs, String destBasePath) throws IOException {
		List<String> response = new ArrayList<String>();
		FileStatus file = fs.getFileStatus(path);
		if (file != null && file.isFile()) {
			response.add(trimExtension(file.getPath().toUri().getPath(), destBasePath));
			return response;
		}

		FileStatus[] fstats = fs.listStatus(path);

		if (fstats != null && fstats.length > 0) {

			for (FileStatus fstat : fstats) {

				if (fstat.isDirectory()) {
					response.addAll(getFileStatusRecursive(fstat.getPath(), fs, destBasePath));
				} else {
					response.add(trimExtension(fstat.getPath().toUri().getPath(), destBasePath));
				}
			}
		}
		return response;
	}

	private static void printUsageAndExit() {

		System.out.println("Snapshot : Usage: yarn jar <jar location> "
				+ "<srcPath> <destPath> <srcNN> <destNN> <local reportpath> <local reportpath>");
		System.out
				.println("Example : yarn jar <jar location> /projects /backup hdfs://prod-fdphadoop-bheema-nn-0001:8020 hdfs://prod-backup-nn-0001:8020 projects.fstat");
		System.exit(1);
	}

	public static void main(String args[]) throws Exception {

		if (args.length < 4) {
			printUsageAndExit();
		}
		int exitCode = ToolRunner.run(new FileCountDriver(), args);
		System.exit(exitCode);
	}
}
