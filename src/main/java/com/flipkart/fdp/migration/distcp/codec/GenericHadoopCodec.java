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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;

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

	public List<FileStatus> getInputPaths(String path,
			Collection<String> excludeList) throws Exception {

		String paths[] = path.split(",");
		return getInputPaths(Arrays.asList(paths), excludeList);
	}

	public List<FileStatus> getInputPaths(Collection<String> paths,
			Collection<String> excludeList) throws Exception {

		List<FileStatus> fileList = new ArrayList<FileStatus>();
		System.out.println("A total of " + paths.size() + " paths to scan...");
		for (String path : paths) {

			List<FileStatus> fstat = getFileStatusRecursive(new Path(path),
					excludeList);
			fileList.addAll(fstat);
		}
		return fileList;
	}

	public void close() throws IOException {
		IOUtils.closeStream(fs);

	}

	public List<FileStatus> getFileStatusRecursive(Path path,
			Collection<String> excludeList) throws MalformedURLException,
			IOException, AuthenticationException {

		List<FileStatus> response = new ArrayList<FileStatus>();

		FileStatus file = fs.getFileStatus(path);
		if (file != null && file.isFile()) {
			response.add(file);
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

					response.add(fstat);
				}
			}
		}
		return response;
	}
}
