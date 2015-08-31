package com.flipkart.fdp.migration.distcp.codec;

import com.flipkart.fdp.migration.distcp.config.*;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.migration.distftp.DistFTPClient;
import com.flipkart.fdp.migration.distftp.DistFTPInputSplit;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;


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

   private List<DistFTPClient> distFTPClient = null;
   private long totalBatchSize = 0l;


    public GenericFTPCodec(Configuration conf,List<HostConfig> hostConfigList) throws Exception {
        int index = 0;
        long availableSize;
        long requiredSize;

        List<DistFTPInputSplit> splits = new ArrayList<DistFTPInputSplit>();
        Gson gson = new Gson();
        OptimizedWorkloadConfig optimizedWorkloadConfig = gson.fromJson(conf.get("split_tasks"), OptimizedWorkloadConfig.class);
        InputFileMapConfig inputFileMapConfig = gson.fromJson("input_map",InputFileMapConfig.class);

        for( Set<IInputJob> stats : optimizedWorkloadConfig.getSplitTasks() ) {
            requiredSize = 0l;
            availableSize = hostConfigList.get(index).getFreeSpaceInBytes();
            List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
            for (IInputJob stat : stats) {
                tuple.add(inputFileMapConfig.getInputFileMap().get(stat.getJobKey()));
                requiredSize += stat.getJobSize();
            }
            totalBatchSize += requiredSize;
            if( requiredSize > availableSize )
                throw new Exception("Total Files size is more than available space on disk! ");
            else
                splits.add(new DistFTPInputSplit(tuple,requiredSize, new HostConfig(hostConfigList.get(index++))));

        }

        distFTPClient = Lists.newArrayList();
        for(DistFTPInputSplit distFTPInputSplit : splits) {
            for (HostConfig hostConfig : hostConfigList) {
                for (MirrorDCMImpl.FileTuple file : distFTPInputSplit.getSplits()) {
                    String[] filePathTree = file.fileName.split("/");
                    this.distFTPClient.add(new DistFTPClient(constructFtpURLString(hostConfig, filePathTree[filePathTree.length - 1]), conf));
                }
            }
        }

   }

    private URI constructFtpURLString(HostConfig config,String destFileName) throws URISyntaxException {
        System.out.println("URI : "+String.valueOf(
                DCMConstants.FTP_DEFAULT_PROTOCOL
                        + config.getUserName() + ":" + config.getUserPassword()
                        + "@" + config.getHost() + config.getDestPath() + destFileName));
        return new URI( String.valueOf(
                            DCMConstants.FTP_DEFAULT_PROTOCOL
                            + config.getUserName() + ":" + config.getUserPassword()
                            + "@" + config.getHost() + config.getDestPath() + destFileName)
                        );
    }


    @Override
    public List<OutputStream> createOutputStream(Configuration conf, String path, boolean append) throws IOException {
        List<OutputStream> outputStreams = Lists.newArrayList();
        for(DistFTPClient ftpClient : distFTPClient){
            outputStreams.add(ftpClient.getOutputStream());
        }
        return outputStreams;
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
        for (DistFTPClient ftpClient : distFTPClient)
            ftpClient.close();
    }


    public List<DistFTPClient> getDistFTPClient() {
        return distFTPClient;
    }

    public void setDistFTPClient(List<DistFTPClient> distFTPClient) {
        this.distFTPClient = distFTPClient;
    }

    public long getTotalBatchSize() {
        return totalBatchSize;
    }

    public void setTotalBatchSize(long totalBatchSize) {
        this.totalBatchSize = totalBatchSize;
    }
}

