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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;

public class MirrorInputSplit extends InputSplit implements Writable {

	private List<FileTuple> splits = null;
	private long length = 0;

	public MirrorInputSplit() {
		splits = new ArrayList<FileTuple>();
		length = 0;
	}

	public MirrorInputSplit(List<FileTuple> splits, long size) {
		this.splits = splits;
		this.length = size;
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		splits = new ArrayList<FileTuple>(size);
		for (int i = 0; i < size; i++) {
			String path = Text.readString(in);
			long len = in.readLong();
			splits.add(new FileTuple(path, len));
		}
		length = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		int size = splits.size();
		out.writeInt(size);
		for (FileTuple split : splits) {
			Text.writeString(out, split.fileName);
			out.writeLong(split.size);
		}
		out.writeLong(length);
	}

	@Override
	public long getLength() throws IOException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] {};
	}

	public void setLength(long length) {
		this.length = length;
	}

	public List<FileTuple> getSplits() {
		return splits;
	}

	public void setSplits(List<FileTuple> splits) {
		this.splits = splits;
	}

}
