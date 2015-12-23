package com.flipkart.fdp.migration.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FileTuple implements Writable {

	private String fileName;
	private long size;
	private long ts;

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

	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public long getSize() {
		return size;
	}
	
	public void setSize(long size) {
		this.size = size;
	}
	
	public long getTs() {
		return ts;
	}
	
	public void setTs(long ts) {
		this.ts = ts;
	}
	
}