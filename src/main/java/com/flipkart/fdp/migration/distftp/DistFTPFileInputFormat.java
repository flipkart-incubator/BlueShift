package com.flipkart.fdp.migration.distftp;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by sushil.s on 28/08/15.
 */
public class DistFTPFileInputFormat extends InputFormat<Text, Text> {

    public static final String DCM_CONFIG = "dcm_config";
    public static final String INCLUDE_FILES = "include_files";
    public static final String EXCLUDE_FILES = "exclude_files";

    private DCMCodec dcmCodec = null;
    private Configuration conf = null;
    private DCMConfig dcmConfig = null;
    private StateManager stateManager = null;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Set<String> excludeList;
        Set<String> includeList;
        Map<String, TransferStatus> previousState;

        System.out.println("Calculating Job Splits...");
        conf = context.getConfiguration();
        dcmConfig = MirrorUtils.getConfigFromConf(conf);

        dcmCodec = DCMCodecFactory.getSourceCodec(conf, dcmConfig.getSourceConfig()
                .getConnectionConfig());

        excludeList = getExclusionsFileList(conf);
        includeList = getInclusionFileList(conf);
        HashMap<String, MirrorDCMImpl.FileTuple> inputFileMap = new HashMap<String, MirrorDCMImpl.FileTuple>();

        List<InputSplit> splits = new ArrayList<InputSplit>();
        Set<OptimTuple> locations = new HashSet<OptimTuple>();

        long totalBatchSize = 0;
        try {
            System.out.println("Scanning source location...");
            List<MirrorDCMImpl.FileTuple> fstats = null;
            if (includeList != null && includeList.size() > 0)
                fstats = dcmCodec.getInputPaths(includeList, excludeList);
            else
                fstats = dcmCodec.getInputPaths(dcmConfig.getSourceConfig()
                        .getPath(), excludeList);

            stateManager = StateManagerFactory.getStateManager(conf, dcmConfig);

            System.out
                    .println("Fetching previous transfer states from StateManager...");
            previousState = stateManager.getPreviousTransferStatus();

            System.out
                    .println("Filtering Input File Set based on User defined filters.");
            for (MirrorDCMImpl.FileTuple fstat : fstats) {

                if (!ignoreFile(fstat, excludeList, previousState)) {

                    locations.add(new OptimTuple(fstat.fileName, fstat.size));
                    inputFileMap.put(fstat.fileName, fstat);
                }
            }
            System.out.println("Optimizing Splits...");
            int numWorkers = MirrorUtils.getNumOfWorkers(dcmConfig, locations.size());
            if (numWorkers > 0 ) {

                numWorkers = dcmConfig.getNumWorkers();
                Gson gson = new Gson();
                conf.set("split_tasks",gson.toJson(optimizeWorkload(locations,
                        numWorkers)));
                conf.set("input_map",gson.toJson(inputFileMap));

            } else {
                for (OptimTuple stat : locations) {
                    List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
                    tuple.add(inputFileMap.get(stat.getJobKey()));
                    splits.add(new MirrorInputSplit(tuple, stat.getJobSize()));
                    totalBatchSize += stat.getJobSize();
                }
            }
            if (splits.size() <= 0)
                throw new Exception("No Inputs Identified for Processing.. ");

            sortSplits(splits);
            System.out.println("Total input paths to process: "
                    + locations.size() + ", Total input splits: "
                    + splits.size());
            System.out.println("Total Data to Transfer: " + totalBatchSize);

            stateManager.savePreiviousTransferStatus(previousState);
        } catch (Exception e) {
            throw new IOException(e);
        }

        System.out.println("Done Calculating splits...");
        return splits;
    }

    private void sortSplits(List<InputSplit> splits) {
        Collections.sort(splits, new Comparator<InputSplit>() {
            // @Override
            public int compare(InputSplit f0, InputSplit f1) {
                try {
                    if (f1.getLength() > f0.getLength())
                        return 1;
                    if (f1.getLength() < f0.getLength())
                        return -1;
                    return 0;
                } catch (Exception e) {
                    return 0;
                }
            }
        });
    }

    private boolean ignoreFile(MirrorDCMImpl.FileTuple fileStat, Set<String> excludeList,
                               Map<String, TransferStatus> previousState) {


        // File Size Based Rules
        long fileSize = fileStat.size;
        if (fileSize <= 0 && dcmConfig.getSourceConfig().isIgnoreEmptyFiles())
            return true;

        if (fileSize < dcmConfig.getSourceConfig().getMinFilesize())
            return true;

        if (dcmConfig.getSourceConfig().getMaxFilesize() != -1
                && fileSize > dcmConfig.getSourceConfig().getMaxFilesize())
            return true;

        // File Time Based rules
        long ts = dcmConfig.getSourceConfig().getStartTS();
        if (ts > 0 && fileStat.ts < ts)
            return true;

        ts = dcmConfig.getSourceConfig().getEndTS();
        if (ts > 0 && fileStat.ts > ts)
            return true;

        // File Name based rules
        String path = fileStat.fileName;
        if (excludeList.contains(path))
            return true;

        if (previousState.containsKey(path)) {
            TransferStatus details = previousState.get(path);
            if (details.getStatus() == Status.COMPLETED) {
                return !(dcmConfig.getSourceConfig().isIncludeUpdatedFiles()
                        && details.getTs() < fileStat.ts);

            }
        }
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
                                                       TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new DistFTPMirrorFileRecordReader();
    }

    public static void setExclusionsFileList(Configuration conf,
                                             Collection<String> files) {
        conf.set(DistFTPFileInputFormat.EXCLUDE_FILES,
                MirrorUtils.getListAsString(files));

    }

    public static void setInclusionFileList(Configuration conf,
                                            Collection<String> files) {
        conf.set(DistFTPFileInputFormat.INCLUDE_FILES,
                MirrorUtils.getListAsString(files));
    }

    public static Set<String> getExclusionsFileList(Configuration conf) {

        return MirrorUtils.getStringAsLists(conf.get(EXCLUDE_FILES));

    }

    public static Set<String> getInclusionFileList(Configuration conf) {
        return MirrorUtils.getStringAsLists(conf.get(INCLUDE_FILES));
    }

    public List<Set<IInputJob>> optimizeWorkload(Set<OptimTuple> tasks,
                                                 int numMappers) {

        System.out.println("Total Tasks: " + tasks.size() + " Total Mappers: "
                + numMappers);
        JobLoadOptimizerFactory.Optimizer optimizer;
        if ( dcmConfig.getSinkConfig().getConnectionConfig().getConnectionParams().equals(DCMConstants.DIST_FTP_CONN_PARAM) )
             optimizer = JobLoadOptimizerFactory.Optimizer.PTASOPTMIZER;
        else
            optimizer = JobLoadOptimizerFactory.Optimizer.PRIORITY_QUEUE_BASED;

        System.out.println("Using Optimizer: " + optimizer.toString());
        IJobLoadOptimizer iJobLoadOptimizer = JobLoadOptimizerFactory
                .getJobLoadOptimizerFactory(optimizer);
        List<Set<IInputJob>> optimizedLoadSets = iJobLoadOptimizer
                .getOptimizedLoadSets(tasks, numMappers);
        return optimizedLoadSets;
    }

}
