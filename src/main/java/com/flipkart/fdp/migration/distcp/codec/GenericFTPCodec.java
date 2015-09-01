package com.flipkart.fdp.migration.distcp.codec;

import com.flipkart.fdp.migration.distcp.config.*;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.migration.FSclients.MultiFTPClient;
import org.apache.hadoop.conf.Configuration;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by sushil.s on 31/08/15.
 */
public class GenericFTPCodec implements  DCMCodec {

   private HostConfig hostConfig = null;
   private MultiFTPClient multiFTPClient = null;
   private long totalBatchSize = 0l;
   private Configuration conf = null;

    public GenericFTPCodec(Configuration conf,MirrorInputSplit mirrorInputSplit) throws Exception {
       this.conf = conf;
       this.hostConfig = mirrorInputSplit.getHostConfig();
    }

    private URI constructFtpURLString(String destFileName) throws URISyntaxException {
/*        System.out.println("URI : "+String.valueOf(
                DCMConstants.FTP_DEFAULT_PROTOCOL
                        + hostConfig.getUserName() + ":" + hostConfig.getUserPassword()
                        + "@" + hostConfig.getHost() + hostConfig.getDestPath() + destFileName));*/
        return new URI( String.valueOf(
                            DCMConstants.FTP_DEFAULT_PROTOCOL
                            + hostConfig.getUserName() + ":" + hostConfig.getUserPassword()
                            + "@" + hostConfig.getHost() + hostConfig.getDestPath() + destFileName)
                        );
    }


    @Override
    public OutputStream createOutputStream(Configuration conf, String path, boolean append) throws IOException {
        try {
            String[] pathTree = path.split("/");
            multiFTPClient = new MultiFTPClient(constructFtpURLString(pathTree[pathTree.length-1]),conf);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return multiFTPClient.getOutputStream();
    }

    @Override
    public InputStream createInputStream(Configuration conf, String path) throws IOException {
        throw new IOException("Creating Inputstream for Multi-FTP not supported!");
    }

    @Override
    public boolean deleteSoureFile(String path) throws IOException {
        return false;
    }

    @Override
    public boolean isSplitable() {
        return false;
    }

    @Override
    public boolean isExistsPath(String path) throws IOException {
        return false;
    }

    @Override
    public List<MirrorDCMImpl.FileTuple> getInputPaths(String path, Collection<String> excludeList) throws Exception {
        return null;
    }

    @Override
    public List<MirrorDCMImpl.FileTuple> getInputPaths(Collection<String> paths, Collection<String> excludeList) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }


    public MultiFTPClient getMultiFTPClient() {
        return multiFTPClient;
    }

    public void setMultiFTPClient(MultiFTPClient multiFTPClient) {
        this.multiFTPClient = multiFTPClient;
    }

    public long getTotalBatchSize() {
        return totalBatchSize;
    }

    public void setTotalBatchSize(long totalBatchSize) {
        this.totalBatchSize = totalBatchSize;
    }


    public HostConfig getHostConfig() {
        return hostConfig;
    }

    public void setHostConfig(HostConfig hostConfig) {
        this.hostConfig = hostConfig;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}

