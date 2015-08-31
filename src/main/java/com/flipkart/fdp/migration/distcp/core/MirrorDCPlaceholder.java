package com.flipkart.fdp.migration.distcp.core;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.MD5Digester;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.flipkart.fdp.migration.distftp.DistFTPInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by sushil.s on 31/08/15.
 */
public class MirrorDCPlaceholder {

    private String srcPath = null;


    private DistFTPInputSplit fSplit = null;

    private DCMConfig dcmConfig = null;
    private Configuration conf = null;


    private List<MirrorDCMImpl.FileTuple> inputs = null;
    private int index = 0;

    private InputStream in = null;
    private List<OutputStream> out = null;

    private DCMCodec inCodec = null;

    private String taskId = null;

    private TaskAttemptContext context = null;

    private DCMCodec outCodec = null;
    private StateManager stateManager = null;
    private TransferStatus status = null;
    private Map<String, TransferStatus> transferStatus = null;
    private MD5Digester digest = null;

    private MirrorDCMImpl.FileTuple current = null;


    public  void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        this.context = context;
        this.conf = context.getConfiguration();
        dcmConfig = MirrorUtils.getConfigFromConf(conf);
        fSplit = (DistFTPInputSplit) inputSplit;
        inputs = fSplit.getSplits();
        stateManager = StateManagerFactory.getStateManager(conf, MirrorUtils.getConfigFromConf(conf));
        transferStatus = stateManager.getTransferStatus(context.getTaskAttemptID().getTaskID().toString());



        System.out.println("Initializing transfer of : " + inputs.size()
                + " files, with a total Size of : " + fSplit.getLength());

        inCodec = DCMCodecFactory.getSourceCodec(conf, dcmConfig.getSourceConfig().getConnectionConfig());

        outCodec = DCMCodecFactory.getDestinationCodec(conf,dcmConfig.getSinkConfig().getConnectionConfig(),inputSplit);
    }


    private void initializeStreams(String destPath) throws IOException{

        status.setInputPath(srcPath);
        status.setOutputPath(destPath);

        if (status.isInputTransformed()) {
            destPath = MirrorUtils.stripExtension(destPath);
            status.setOutputPath(destPath);

            in = inCodec.createInputStream(conf, srcPath);
            in = MirrorUtils.getCodecInputStream(conf, dcmConfig, srcPath, in);
        } else {
            in = inCodec.createInputStream(conf, srcPath);
        }

        if (status.isOutputCompressed()) {
            destPath = destPath + "."
                    + dcmConfig.getSinkConfig().getCompressionCodec();
            status.setOutputPath(destPath);

            if (!dcmConfig.getSinkConfig().isOverwriteFiles()) {
                if (outCodec.isExistsPath(destPath)) {
                    throw new FileAlreadyExistsException(destPath);
                }
            }

            for(OutputStream outputStream : outCodec.createOutputStream(conf, destPath, dcmConfig
                    .getSinkConfig().isAppend()))
                out.add(MirrorUtils.getCodecOutputStream(conf, dcmConfig, destPath,
                            outputStream));
        } else {
            out = outCodec.createOutputStream(conf, destPath, dcmConfig
                    .getSinkConfig().isAppend());
        }

        String statusMesg = "Processing: " + srcPath + " -> " + destPath;
        context.setStatus(statusMesg);
        System.out.println("Status: " + statusMesg);
    }

    public int proceed() throws IOException {
        digest = null;
        current = inputs.get(index);
        srcPath = MirrorUtils.getSimplePath(new Path(current.fileName));
        status = checkTransferStatus();

        //TODO://Need to remove this hack to
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

                initializeStreams(destPath);

                digest = MirrorUtils.copy(in, out, context);

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
        return  index;
    }


    private TransferStatus checkTransferStatus() {

        TransferStatus stat = transferStatus.get(srcPath);
        if (stat != null && stat.getStatus() == Status.COMPLETED) {
            return stat;
        }
        return null;
    }


    private void updateStatus() throws IOException {

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


    public void closeStreams() throws IOException {

        IOUtils.closeStream(in);
        for(OutputStream outputStream : out)
            IOUtils.closeStream(outputStream);

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

    public int getSplitSize() {
        return inputs.size();
    }

    public float getCompletedSplits() {
        return index;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public String getStatus() {
        return status.toString();
    }
}
