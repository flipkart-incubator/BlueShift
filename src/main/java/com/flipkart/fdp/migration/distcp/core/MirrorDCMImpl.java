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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.flipkart.fdp.migration.distcp.config.DCMConstants;

public class MirrorDCMImpl {

	public static class MirrorMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class MirrorReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values)
				context.write(key, value);
		}
	}

    //Todo://Make a separate calss with private fields
	public static class FileTuple implements Writable {
		public String fileName;
		public long size;
		public long ts;

		public FileTuple() {

		}

		public FileTuple(String path, long len, long ts) {
			this.fileName = path;
			this.size = len;
			this.ts = ts;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, fileName);
			out.writeLong(size);
			out.writeLong(ts);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			fileName = Text.readString(in);
			size = in.readLong();
			ts = in.readLong();
		}

	}

	public static enum BLUESHIFT_COUNTER {
		SUCCESS_COUNT, FAILED_COUNT
	};

	public static class FileTupleComparator implements Comparator<FileTuple> {

		// @Override
		public int compare(FileTuple f0, FileTuple f1) {

			if (f1.size > f0.size)
				return 1;
			if (f1.size < f0.size)
				return -1;
			return 0;
		}

	}

	public static void HackMapreduce() throws Exception {

		DCMConstants.setFinalStatic(
				org.apache.hadoop.mapreduce.lib.input.FileInputFormat.class
						.getDeclaredField("hiddenFileFilter"),
				new PathFilter() {
					public boolean accept(Path p) {
						return true;
					}
				});
		DCMConstants.setFinalStatic(
				org.apache.hadoop.mapred.FileInputFormat.class
						.getDeclaredField("hiddenFileFilter"),
				new PathFilter() {
					public boolean accept(Path p) {
						return true;
					}
				});
	}

}
