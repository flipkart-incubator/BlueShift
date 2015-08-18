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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

public class GenericHadoopCodec implements DCMCodec {

	private FileSystem fs = null;

	public GenericHadoopCodec(FileSystem fs) {

		this.fs = fs;
	}

	public OutputStream createOutputStream(Configuration conf, String path,
			boolean append) throws IOException {
		if (append)
			return fs.append(new Path(path));
		else
			return fs.create(new Path(path));
	}

	public InputStream createInputStream(Configuration conf, String path)
			throws IOException {

		return fs.open(new Path(path));

	}

	public boolean deleteSoureFile(String path) throws IOException {
		return fs.delete(new Path(path), false);
	}

	public boolean isSplitable() {

		return false;
	}

	public List<FileStatus> getInputPaths(String path) throws Exception {
		List<FileStatus> fstats = new ArrayList<FileStatus>();

		String paths[] = path.split(",");
		for (String file : paths) {
			try {
				List<FileStatus> fstat = getFileStatusRecursive(new Path(file));
				fstats.addAll(fstat);
			} catch (Exception e) {
				e.printStackTrace();
				// Ignore Exception
			}
		}
		return fstats;
	}

	public List<FileStatus> getInputPaths(Collection<String> paths)
			throws Exception {
		List<FileStatus> fileList = new ArrayList<FileStatus>();

		for (String path : paths) {
			try {
				FileStatus stat = fs.getFileStatus(new Path(path));
				if (stat != null)
					fileList.add(stat);
			} catch (Exception e) {
				e.printStackTrace();
				// Ignore Exception
			}
		}
		return fileList;
	}

	public void close() throws IOException {
		IOUtils.closeStream(fs);

	}

	public List<FileStatus> getFileStatusRecursive(Path path)
			throws MalformedURLException, IOException, AuthenticationException {

		List<FileStatus> response = new ArrayList<FileStatus>();

		FileStatus[] fstats = fs.listStatus(path);

		if (fstats != null && fstats.length > 0) {

			for (FileStatus fstat : fstats) {

				if (fstat.isDirectory()) {

					response.addAll(getFileStatusRecursive(fstat.getPath()));
				} else {

					response.add(fstat);
				}
			}
		}
		return response;
	}
}
