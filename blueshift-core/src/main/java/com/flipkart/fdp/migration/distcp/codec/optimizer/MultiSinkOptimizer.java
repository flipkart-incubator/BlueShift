package com.flipkart.fdp.migration.distcp.codec.optimizer;

import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.*;

public class MultiSinkOptimizer implements WorkloadOptimizer {

	@Override
	public List<InputSplit> optimizeWorkload(DCMConfig dcmConfig,
			Set<OptimTuple> locations, HashMap<String, FileTuple> inputFileMap)
			throws IOException {

		int numWorkers = dcmConfig.getNumWorkers();//getSinkConfig().getConnectionConfig().size();
		int index = 0;
        int parallelismCount;
		long availableSize;
		long requiredSize;
		long totalBatchSize = 0l;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<ConnectionConfig> hostConfigList = dcmConfig.getSinkConfig()
				.getConnectionConfig();
        int numOfSinks = hostConfigList.size();

                Collections.sort(hostConfigList, new Comparator<ConnectionConfig>() {
			@Override
			public int compare(ConnectionConfig o1, ConnectionConfig o2) {
				return o1.compareTo(o2);

			}
		});

		Collections.reverse(hostConfigList);
        if( inputFileMap.size() < numOfSinks ){
            numWorkers = inputFileMap.size();
            parallelismCount = 1;
        }else if (inputFileMap.size() < numWorkers) {
            numWorkers = inputFileMap.size();
            parallelismCount = inputFileMap.size() / numOfSinks;
        }else{
            parallelismCount =  dcmConfig.getNumWorkers() / numOfSinks;
        }

        List<Set<IInputJob>> optimizeWorkload = OptimizerUtils.optimizeWorkload(
                        JobLoadOptimizerFactory.Optimizer.PTASOPTMIZER, locations,
                        numWorkers);

        int i;
        for( i = 0 ; i < optimizeWorkload.size() ; i=i+parallelismCount){

            if( index == numOfSinks ) // this is for enabling round robin TODO: tackle fragmentation
                index = 0;

            requiredSize = 0l;
            availableSize = hostConfigList.get(index).getFreeSpaceInBytes();
            List<List<FileTuple>> parallelisedTuple = Lists.newArrayList();

            for( int j = 0 ; j < parallelismCount && (i+j) < optimizeWorkload.size(); j++ ){
                List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
                Set<IInputJob> stats = optimizeWorkload.get(i+j);
                for (IInputJob stat : stats) {
                    tuple.add(inputFileMap.get(stat.getJobKey()));
                    requiredSize += stat.getJobSize();
                }

                if( tuple.size() > 0 )
                    parallelisedTuple.add(tuple);
            }

            totalBatchSize += requiredSize;
            if (requiredSize > availableSize)
                throw new IOException(
                        "Total Files size is more than available space on disk! ");
            else{
                for( List<FileTuple> fileTuples : parallelisedTuple ) {
                    splits.add(new MirrorInputSplit(fileTuples, requiredSize, dcmConfig
                            .getSourceConfig().getDefaultConnectionConfig(),
                            hostConfigList.get(index)));
                }
            }
            index++;
        }

		System.out.println("Total Batch Size : " + totalBatchSize);

		return splits;
	}
}
