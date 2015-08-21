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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public interface DCMCodec extends Closeable {

	public OutputStream createOutputStream(Configuration conf, String path,
			boolean append) throws IOException;

	public InputStream createInputStream(Configuration conf, String path)
			throws IOException;

	public boolean deleteSoureFile(String path) throws IOException;

	public boolean isSplitable();

	public List<FileStatus> getInputPaths(String path,
			Collection<String> excludeList) throws Exception;

	public List<FileStatus> getInputPaths(Collection<String> paths,
			Collection<String> excludeList) throws Exception;
}
