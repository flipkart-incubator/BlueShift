package com.flipkart.fdp.migration.distcp.codec.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;

public class SingleSinkOptimizer implements WorkloadOptimizer {

	@Override
	public List<InputSplit> optimizeWorkload(DCMConfig dcmConfig,
			Set<OptimTuple> locations, HashMap<String, FileTuple> inputFileMap)
			throws IOException {

		int numWorkers = locations.size();
		long totalBatchSize = 0l;
		List<InputSplit> splits = new ArrayList<InputSplit>();

		if (dcmConfig.getNumWorkers() > 0
				&& dcmConfig.getNumWorkers() < numWorkers) {

			numWorkers = dcmConfig.getNumWorkers();

			for (Set<IInputJob> stats : OptimizerUtils.optimizeWorkload(
					JobLoadOptimizerFactory.Optimizer.PRIORITY_QUEUE_BASED,
					locations, numWorkers)) {
				List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
				long size = 0;
				for (IInputJob stat : stats) {
					tuple.add(inputFileMap.get(stat.getJobKey()));
					size += stat.getJobSize();
				}
				totalBatchSize += size;
				splits.add(new MirrorInputSplit(tuple, size, dcmConfig
						.getSourceConfig().getDefaultConnectionConfig(),
						dcmConfig.getSinkConfig().getDefaultConnectionConfig()));
			}
		} else {
			for (OptimTuple stat : locations) {
				List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
				tuple.add(inputFileMap.get(stat.getJobKey()));
				splits.add(new MirrorInputSplit(tuple, stat.getJobSize(),
						dcmConfig.getSourceConfig()
								.getDefaultConnectionConfig(), dcmConfig
								.getSinkConfig().getDefaultConnectionConfig()));
				totalBatchSize += stat.getJobSize();
			}
		}
		System.out.println("Total Batch Size : " + totalBatchSize);
		return splits;
	}
}
