package com.flipkart.fdp.migration.distftp;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.HostConfig;
import com.flipkart.fdp.migration.distcp.config.MD5Digester;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by sushil.s on 28/08/15.
 */
@Slf4j
public class DistFTPMirrorFileRecordReader extends RecordReader<Text, Text> {


    private boolean read = false;

    private String srcPath = null;

    private Text key = new Text();
    private Text value = new Text();

    private DistFTPInputSplit fSplit = null;

    private DCMConfig dcmConfig = null;
    private Configuration conf = null;
    private StateManager stateManager = null;

    private List<MirrorDCMImpl.FileTuple> inputs = null;
    private int index = 0;

    private InputStream in = null;

    private DCMCodec inCodec = null;

    private TransferStatus status = null;

    private Map<String, TransferStatus> transferStatus = null;

    private String taskId = null;

    private TaskAttemptContext context = null;

    private DistFTPClient ftpClient = null;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        this.context = taskAttemptContext;
        this.conf = context.getConfiguration();
        dcmConfig = MirrorUtils.getConfigFromConf(conf);
        taskId = context.getTaskAttemptID().getTaskID().toString();

        stateManager = StateManagerFactory.getStateManager(conf, dcmConfig);
        transferStatus = stateManager.getTransferStatus(taskId);
        read = false;


        fSplit = (DistFTPInputSplit) inputSplit;
        inputs = fSplit.getSplits();
        System.out.println("Initializing transfer of : " + inputs.size()
                + " files, with a total Size of : " + fSplit.getLength());

        inCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSourceConfig()
                .getConnectionConfig());

        ftpClient = new DistFTPClient();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!read) {
            MD5Digester digest = null;
            MirrorDCMImpl.FileTuple current = inputs.get(index);
            srcPath = MirrorUtils.getSimplePath(new Path(current.fileName));
            status = checkTransferStatus();
            String destPath = fSplit.getHostConfig().getDestPath();

            if (status == null) {

                status = new TransferStatus();
                status.setStatus(Status.NEW);

                status.setInputSize(current.size);
                status.setTs(current.ts);
                status.setTaskID(taskId);
                System.out.println("Transfering file: " + current.fileName
                        + ", with size: " + current.size);

                try {

                    analyzeStrategy();

                    String[] filename = srcPath.split("/");
                    destPath = destPath + filename[filename.length-1];

                    initializeStreams(destPath);

                    ftpClient.connect(fSplit.getHostConfig());

                    digest = MirrorUtils.copy(destPath,in,context,ftpClient);

                    status.setStatus(Status.COMPLETED);
                    status.setMd5Digest(digest.getDigest());
                    status.setOutputSize(digest.getByteCount());

                } catch (Exception e) {

                    System.err.println("Error processing input: "
                            + e.getMessage());
                    e.printStackTrace();
                    status.setStatus(Status.FAILED);
                    if (!dcmConfig.isIgnoreException()) {
                        throw new IOException(e);
                    }
                }
                closeStreams();

            }
            updateStatus();
            index++;
            if (index == inputs.size()) {
                context.setStatus("SUCCESS");
                read = true;
            }
            return true;
        } else {
            return false;
        }
    }

    private void initializeStreams(String destPath) throws IOException{

        status.setInputPath(srcPath);
        status.setOutputPath(destPath);

        if (status.isInputTransformed()) {
            status.setOutputPath(destPath);

            in = inCodec.createInputStream(conf, srcPath);
            in = MirrorUtils.getCodecInputStream(conf, dcmConfig, srcPath, in);
        } else {
            in = inCodec.createInputStream(conf, srcPath);
        }

        String statusMesg = "Processing: " + srcPath + " -> " + destPath;
        context.setStatus(statusMesg);
        System.out.println("Status: " + statusMesg);
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return read ? 1 : (((float) inputs.size()) / index);
    }

    @Override
    public void close() throws IOException {
        System.out.println("Transfer Complete...");
        IOUtils.closeStream(inCodec);
    }

    private TransferStatus checkTransferStatus() {

        TransferStatus stat = transferStatus.get(srcPath);
        if (stat != null && stat.getStatus() == Status.COMPLETED) {
            return stat;
        }
        return null;
    }

    private void updateStatus() throws IOException {

        key.set(srcPath);
        value.set(status.toString());

        if (status.getStatus() == Status.COMPLETED) {
            context.getCounter(MirrorDCMImpl.BLUESHIFT_COUNTER.SUCCESS_COUNT).increment(1);
        } else {
            context.getCounter(MirrorDCMImpl.BLUESHIFT_COUNTER.FAILED_COUNT).increment(1);
        }
        try {
            stateManager.updateTransferStatus(status);
        } catch (Exception e) {
            System.err.println("Caught exception persisting state: "
                    + e.getMessage());
        }
    }

    private void analyzeStrategy() {

        String srcCodec = null;

        if (dcmConfig.getSourceConfig().isTransformSource())
            srcCodec = MirrorUtils.getCodecNameFromPath(conf, srcPath);

        if (srcCodec != null)
            status.setInputCompressed(false);


        try {
            if (fSplit.getLength() < dcmConfig.getSourceConfig()
                    .getCompressionThreshold()) {
                status.setInputTransformed(false);
                status.setOutputCompressed(false);
            }
        } catch (Exception e) {
            System.err.println("Error Analyzing Transfer strategy: "
                    + e.getMessage());
        }
    }



    private void closeStreams() throws IOException {

        IOUtils.closeStream(in);
        ftpClient.closeConnection();

        if (status.getStatus() == Status.COMPLETED
                && dcmConfig.getSourceConfig().isDeleteSource()) {
            try {
                inCodec.deleteSoureFile(srcPath);
            } catch (Exception e) {
                System.err.println("Failed Deleting file: " + srcPath
                        + ", Exception: " + e.getMessage());
            }
        }
    }

}
