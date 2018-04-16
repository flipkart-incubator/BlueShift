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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class DCMConstants {

	public static final String BATCH_ID = "batch_id";
	public static final String BATCH_NAME = "batch_name";
	public static final String SOURCE_PATH_KEY = "src_path";
	public static final String INCLUDE_LIST_FILE = "include_list_file";
	public static final String EXCLUDE_LIST_FILE = "exclude_list_file";
	public static final String STATUS_PATH_KEY = "status_path";
	public static final String DESTINATION_PATH_KEY = "dest_path";
	public static final String DESTINATION_HOST_KEY = "dest_host";
	public static final String DESTINATION_PORT_KEY = "dest_port";
	public static final String DESTINATION_USER_NAME_KEY = "user_name";
	public static final String DESTINATION_PASSWORD_KEY = "user_pass";
	public static final String IGNORE_EXCEPTIONS = "ignore_exceptions";
	public static final String OVERWRITE_FILES = "overwrite_files";
	public static final String USE_COMPRESSION = "use_compression";
	public static final String COMPRESSION_CODEC = "compression_codec";
	public static final String DELETE_SOURCE = "delete_source";
	public static final String TRANSFORM_SOURCE = "transform_source";
	public static final String IGNORE_EMPTY_FILES = "ignore_empty_files";
	public static final String START_TIMESTAMP = "start_ts";
	public static final String END_TIMESTAMP = "end_ts";
	public static final String COMPRESSION_THRESHOLD = "compression_threshold";
	public static final String INPLACE_TRANSFORM = "inplace_transform";

	public static final String MAX_FILESIZE = "max_filesize";
	public static final String MIN_FILESIZE = "min_filesize";

	public static final String DB_URL = "db_url";
	public static final String DB_DRIVER = "db_driver";
	public static final String DB_USER_NAME = "db_user_name";
	public static final String DB_USER_PASSWORD = "db_password";

	public static final long HASH_MEDIAN = Long.MAX_VALUE / 2;
	public static HashFunction hasher = Hashing.murmur3_32();

	public static final String HTTP_DEFAULT_PROTOCOL = "http://";
	public static final String HDFS_DEFAULT_PROTOCOL = "hdfs://";
	public static final String HFTP_DEFAULT_PROTOCOL = "hftp://";
	public static final String FTP_DEFAULT_PROTOCOL = "ftp://";
	public static final String HAR_DEFAULT_PROTOCOL = "har://";
	public static final String WEBHDFS_DEFAULT_PROTOCOL = "webhdfs://";
	public static final String GS_DEFAULT_PROTOCOL = "gs://";

	public static void setFinalStatic(Field field, Object newValue)
			throws Exception {
		field.setAccessible(true);

		Field modifiersField = Field.class.getDeclaredField("modifiers");
		modifiersField.setAccessible(true);
		modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

		field.set(null, newValue);
	}

	public static enum FileSystemType {

		WEBHDFS, HDFS, HFTP, FTP, HAR, LOCAL, TCP, HTTP, GS, CUSTOM
	}

	public static enum SecurityType {
		PSEUDO, KERBEROS, SIMPLE, NONE
	}

	public static enum StateManagerImpl {
		HDFS, DB
	}
}
