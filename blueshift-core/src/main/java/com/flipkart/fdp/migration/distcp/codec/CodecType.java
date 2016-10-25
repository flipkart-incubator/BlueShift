package com.flipkart.fdp.migration.distcp.codec;

import java.util.HashMap;
import java.util.Map;

public enum CodecType {
	
	FOUR_MC("4mc", "com.hadoop.compression.fourmc.FourMcCodec"),
	SNAPPY("snappy", "org.apache.hadoop.io.compress.SnappyCodec"),
	GZIP("gz", "org.apache.hadoop.io.compress.GzipCodec"),
	BZIP2("bz2", "org.apache.hadoop.io.compress.BZip2Codec");
	
	private String name;
	
	private String ioCompressionCodecs;
	
	private static Map<String, CodecType> reverseMap = new HashMap<String, CodecType>();
	
	static {
		for(CodecType codecType : CodecType.values()) {
			reverseMap.put(codecType.getName(), codecType);
		}
	}
	
	private CodecType(String name, String ioCompressionCodecs) {
		this.name = name;
		this.ioCompressionCodecs = ioCompressionCodecs;
	}
	
	public String getName() {
		return name;
	}
	
	public static CodecType getCodecType(String name) {
		return reverseMap.get(name);
	}

	public String getIOCompressionCodecs() {
		return this.ioCompressionCodecs;
	}
}