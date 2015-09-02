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

package com.flipkart.fdp.migration.distcp.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import com.flipkart.fdp.migration.distcp.core.MirrorUtils;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;

public class GenericHadoopCodec implements DCMCodec {

	private FileSystem fs = null;

	private Configuration conf = null;

	public GenericHadoopCodec(Configuration conf, FileSystem fs)
			throws Exception {
		this.fs = fs;
		this.conf = conf;
	}

	public OutputStream createOutputStream(String path, boolean append)
			throws IOException {
		if (append)
			return fs.append(new Path(path));
		else
			return fs.create(new Path(path));
	}

	public InputStream createInputStream(String path) throws IOException {

		return fs.open(new Path(path));

	}

	public boolean deleteSoureFile(String path) throws IOException {
		return fs.delete(new Path(path), false);
	}

	public boolean isSplitable() {

		return false;
	}

	public List<FileTuple> getInputPaths(String path,
			Collection<String> excludeList) throws Exception {

		return getInputPaths(Arrays.asList(new String[] { path }), excludeList);
	}

	public List<FileTuple> getInputPaths(Collection<String> paths,
			Collection<String> excludeList) throws Exception {

		System.out.println("A total of " + paths.size() + " paths to scan...");

		List<FileTuple> fileList = new ArrayList<FileTuple>();
		List<String> inputPaths = new ArrayList<String>();

		// Process regular expression based paths
		for (String path : paths) {

			System.out.println("Processing path: " + path);
			FileStatus[] stats = fs.globStatus(new Path(path));

			for (FileStatus fstat : stats) {
				if (fstat.isFile()) {
					fileList.add(new FileTuple(MirrorUtils.getSimplePath(fstat
							.getPath()), fstat.getLen(), fstat
							.getModificationTime()));
				} else {
					inputPaths.add(MirrorUtils.getSimplePath(fstat.getPath()));
				}
			}
		}

		if (inputPaths.size() > 0) {

			for (String path : inputPaths) {

				List<FileTuple> fstat = getFileStatusRecursive(new Path(path),
						excludeList);
				fileList.addAll(fstat);
			}
		}
		return fileList;
	}

	public List<FileTuple> getFileStatusRecursive(Path path,
			Collection<String> excludeList) throws IOException,
			AuthenticationException {

		List<FileTuple> response = new ArrayList<FileTuple>();

		FileStatus file = fs.getFileStatus(path);
		if (file != null && file.isFile()) {
			response.add(new FileTuple(
					MirrorUtils.getSimplePath(file.getPath()), file.getLen(),
					file.getModificationTime()));
			return response;
		}

		FileStatus[] fstats = fs.listStatus(path);

		if (fstats != null && fstats.length > 0) {

			for (FileStatus fstat : fstats) {

				if (fstat.isDirectory()
						&& !excludeList.contains(MirrorUtils
								.getSimplePath(fstat.getPath()))) {

					response.addAll(getFileStatusRecursive(fstat.getPath(),
							excludeList));
				} else {

					response.add(new FileTuple(MirrorUtils.getSimplePath(fstat
							.getPath()), fstat.getLen(), fstat
							.getModificationTime()));
				}
			}
		}
		return response;
	}

	@Override
	public boolean isExistsPath(String path) throws IOException {
		return fs.exists(new Path(path));
	}

	public void close() throws IOException {
		IOUtils.closeStream(fs);

	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	@Override
	public Configuration getConf() {
		return conf;
	}

}
