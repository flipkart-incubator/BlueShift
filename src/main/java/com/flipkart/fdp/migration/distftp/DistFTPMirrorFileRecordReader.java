package com.flipkart.fdp.migration.distftp;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.config.MD5Digester;
import com.flipkart.fdp.migration.distcp.core.MirrorDCPlaceholder;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import java.io.*;

/**
 * Created by sushil.s on 28/08/15.
 */
@Slf4j
public class DistFTPMirrorFileRecordReader extends RecordReader<Text, Text> {

    private Text key = new Text();
    private Text value = new Text();
    private boolean read = false;
    private final MirrorDCPlaceholder placeholder = new MirrorDCPlaceholder();
    private int size;
    private TaskAttemptContext context = null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        placeholder.initialize(inputSplit, taskAttemptContext);
        size = placeholder.getSplitSize();
        context = taskAttemptContext;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!read) {

            if ( placeholder.proceed() == size ) {
                context.setStatus("SUCCESS");
                read = true;
            }

            key.set(placeholder.getSrcPath());
            value.set(placeholder.getStatus());
            return true;
        } else {
            return false;
        }
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
        return read ? 1 : (((float) placeholder.getSplitSize()) / placeholder.getCompletedSplits());
    }

    @Override
    public void close() throws IOException {
        System.out.println("Transfer Complete...");
        placeholder.closeStreams();
    }


}
