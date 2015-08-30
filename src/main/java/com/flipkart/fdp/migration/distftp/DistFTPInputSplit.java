package com.flipkart.fdp.migration.distftp;

import com.flipkart.fdp.migration.distcp.config.HostConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sushil.s on 28/08/15.
 */
public class DistFTPInputSplit extends InputSplit implements Writable {

    private List<MirrorDCMImpl.FileTuple> splits = null;
    private long length = 0;
    private HostConfig hostConfig = null;

    public DistFTPInputSplit() {
        splits = new ArrayList<MirrorDCMImpl.FileTuple>();
        length = 0;
    }

    public DistFTPInputSplit(List<MirrorDCMImpl.FileTuple> splits, long size,HostConfig hostConfig) {
        this.splits = splits;
        this.length = size;
        this.hostConfig = hostConfig;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        splits = new ArrayList<MirrorDCMImpl.FileTuple>(size);
        for (int i = 0; i < size; i++) {
            MirrorDCMImpl.FileTuple ft = new MirrorDCMImpl.FileTuple();
            ft.readFields(in);
            splits.add(ft);
        }
        length = in.readLong();
        HostConfig config = new HostConfig();
        config.readFields(in);
        hostConfig = config;
    }

    public void write(DataOutput out) throws IOException {
        int size = splits.size();
        out.writeInt(size);
        for (MirrorDCMImpl.FileTuple split : splits) {
            split.write(out);
        }
        out.writeLong(length);
        if(hostConfig != null)
            hostConfig.write(out);
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

    public List<MirrorDCMImpl.FileTuple> getSplits() {
        return splits;
    }

    public void setSplits(List<MirrorDCMImpl.FileTuple> splits) {
        this.splits = splits;
    }

    public HostConfig getHostConfig() {
        return hostConfig;
    }

    public void setHostConfig(HostConfig hostConfig) {
        this.hostConfig = hostConfig;
    }
}
