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

import org.apache.hadoop.conf.Configurable;

import com.flipkart.fdp.migration.vo.FileTuple;

public interface DCMCodec extends Closeable, Configurable {

	public OutputStream createOutputStream(String path, boolean useCompression,
			String codecName, boolean append,boolean encrypt,byte[] encryptKey,byte[] encryptIV) throws IOException;

	public InputStream createInputStream(String path, boolean useDeCompression,boolean decrypt,byte[] decryptKey,byte[] decryptIV)
			throws IOException;

	public boolean deleteSoureFile(String path) throws IOException;

	public boolean renameFile(String srcPath, String destPath)
			throws IOException;

	public boolean isSplitable();

	public boolean isExistsPath(String path) throws IOException;

	public List<FileTuple> getInputPaths(String path,
			Collection<String> excludeList) throws Exception;

	public List<FileTuple> getInputPaths(Collection<String> paths,
			Collection<String> excludeList) throws Exception;

}
