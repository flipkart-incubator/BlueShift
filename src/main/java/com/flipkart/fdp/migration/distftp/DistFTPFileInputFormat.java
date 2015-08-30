package com.flipkart.fdp.migration.distftp;

import com.flipkart.fdp.migration.db.models.Status;
import com.flipkart.fdp.migration.distcp.codec.DCMCodec;
import com.flipkart.fdp.migration.distcp.codec.DCMCodecFactory;
import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConstants;
import com.flipkart.fdp.migration.distcp.config.HostConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.migration.distcp.state.StateManager;
import com.flipkart.fdp.migration.distcp.state.StateManagerFactory;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;
import com.flipkart.fdp.migration.distcp.utils.MirrorUtils;
import com.flipkart.fdp.optimizer.Driver;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.math.BigInteger;
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
        long availableSize;
        long requiredSize;
        int index = 0;


        System.out.println("Calculating Job Splits...");
        conf = context.getConfiguration();
        dcmConfig = MirrorUtils.getConfigFromConf(conf);

        dcmCodec = DCMCodecFactory.getCodec(conf, dcmConfig.getSourceConfig()
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


            ConnectionConfig sinkConfig = dcmConfig.getSinkConfig().getConnectionConfig();
            List<HostConfig> hostConfigList = sinkConfig.getHostConfigList();
            Collections.sort(hostConfigList , new Comparator<HostConfig>() {
                @Override
                public int compare(HostConfig o1, HostConfig o2) {
                    return o1.compareTo(o2);
                }
            });

           List<HostConfig> reverseSortedHostConfgList = Lists.reverse(hostConfigList); //For giving highest priority to max space available disk

            if ( sinkConfig.getConnectionParams().equals(DCMConstants.DIST_FTP_CONN_PARAM)) {

                int numWorkers = reverseSortedHostConfgList.size();

                if( numWorkers > locations.size() )
                       numWorkers  = locations.size();

                if (numWorkers > 0) {

                    List<Set<IInputJob>> splitTasks = optimizeWorkload(locations,
                            numWorkers);

                    for( Set<IInputJob> stats : splitTasks ) {
                        requiredSize = 0l;
                        availableSize = reverseSortedHostConfgList
                                            .get(index)
                                                .getFreeSpaceInBytes();
                        List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();

                        if ( stats.size() > 0 ) {
                            for (IInputJob stat : stats) {
                                tuple.add(inputFileMap.get(stat.getJobKey()));
                                requiredSize += stat.getJobSize();
                            }
                            totalBatchSize += requiredSize;

                            if (requiredSize > availableSize)
                                throw new Exception("Total Files size is more than available space on disk! ");
                            else
                                splits.add(new DistFTPInputSplit(tuple, requiredSize, new HostConfig(reverseSortedHostConfgList.get(index++))));
                        }
                    }

                }else
                    throw  new Exception("No Host Config defined under Sink Connection config!");
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

    public static Set<String> getExclusionsFileList(Configuration conf) {

        return MirrorUtils.getStringAsLists(conf.get(EXCLUDE_FILES));

    }

    public static Set<String> getInclusionFileList(Configuration conf) {
        return MirrorUtils.getStringAsLists(conf.get(INCLUDE_FILES));
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

        boolean ignoreFile = false;

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
            if (details.getStatus() == Status.COMPLETED
                    && details.getTs() == fileStat.ts)
                return true;
        }
        return false;
    }

    public List<Set<IInputJob>> optimizeWorkload(Set<OptimTuple> tasks,
                                                 int numMappers) {

        System.out.println("Total Tasks: " + tasks.size() + " Total Mappers: "
                + numMappers);
        JobLoadOptimizerFactory.Optimizer optimizer = JobLoadOptimizerFactory.Optimizer.PTASOPTMIZER;

        System.out.println("Using Optimizer: " + optimizer.toString());
        IJobLoadOptimizer iJobLoadOptimizer = JobLoadOptimizerFactory
                .getJobLoadOptimizerFactory(optimizer);

        return iJobLoadOptimizer
                .getOptimizedLoadSets(tasks, numMappers);
    }


    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
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


}
