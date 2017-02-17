package com.flipkart.fdp.migration.distcp.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;


/**
 * Created by kurian.cheeramelil on 01/02/17.
 */
public class FetchFileChecksum {
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        URI uri = new URI(args[0]);
        FileSystem fs = FileSystem.newInstance(uri,conf);
        Path p = new Path(args[1]);
        System.out.println(fs.getFileChecksum(p));
    }
}
