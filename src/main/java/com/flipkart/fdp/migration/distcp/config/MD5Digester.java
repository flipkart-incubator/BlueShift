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

package com.flipkart.fdp.migration.distcp.config;

import java.math.BigInteger;
import java.security.MessageDigest;

import org.apache.hadoop.io.MD5Hash;

public class MD5Digester {

	private MessageDigest digester = null;
	private long byteCount = 0;

	public MD5Digester() {
		digester = MD5Hash.getDigester();
	}

	public void updateMd5digester(byte[] bytes) {
		digester.update(bytes);
		byteCount += bytes.length;
	}

	public void updateMd5digester(byte[] bytes, int offset, int len) {
		digester.update(bytes, offset, len);
		byteCount += len;
	}

	public String getDigest() {
		BigInteger bi = new BigInteger(1, digester.digest());
		String md5 = bi.toString(16);
		while (md5.length() < 32) {
			md5 = "0" + md5;
		}
		return md5;
	}

	public long getByteCount() {
		return byteCount;
	}

}
