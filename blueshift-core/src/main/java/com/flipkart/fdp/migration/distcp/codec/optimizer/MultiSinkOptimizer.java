package com.flipkart.fdp.migration.distcp.codec.optimizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;

import com.flipkart.fdp.migration.distcp.config.ConnectionConfig;
import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl;
import com.flipkart.fdp.migration.distcp.core.MirrorDCMImpl.FileTuple;
import com.flipkart.fdp.migration.distcp.core.MirrorInputSplit;
import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;

public class MultiSinkOptimizer implements WorkloadOptimizer {

	@Override
	public List<InputSplit> optimizeWorkload(DCMConfig dcmConfig,
			Set<OptimTuple> locations, HashMap<String, FileTuple> inputFileMap)
			throws IOException {

		int numWorkers = dcmConfig.getSinkConfig().getConnectionConfig().size();
		int index = 0;
		long availableSize;
		long requiredSize;
		long totalBatchSize = 0l;
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<ConnectionConfig> hostConfigList = dcmConfig.getSinkConfig()
				.getConnectionConfig();

		Collections.sort(hostConfigList, new Comparator<ConnectionConfig>() {
			@Override
			public int compare(ConnectionConfig o1, ConnectionConfig o2) {
				return o1.compareTo(o2);

			}
		});

		Collections.reverse(hostConfigList);
		if (inputFileMap.size() < numWorkers)
			numWorkers = inputFileMap.size();

		for (Set<IInputJob> stats : OptimizerUtils.optimizeWorkload(
				JobLoadOptimizerFactory.Optimizer.PTASOPTMIZER, locations,
				numWorkers)) {
			requiredSize = 0l;
			availableSize = hostConfigList.get(index).getFreeSpaceInBytes();
			List<MirrorDCMImpl.FileTuple> tuple = new ArrayList<MirrorDCMImpl.FileTuple>();
			for (IInputJob stat : stats) {
				tuple.add(inputFileMap.get(stat.getJobKey()));
				requiredSize += stat.getJobSize();
			}
			totalBatchSize += requiredSize;
			if (requiredSize > availableSize)
				throw new IOException(
						"Total Files size is more than available space on disk! ");
			else
				splits.add(new MirrorInputSplit(tuple, requiredSize, dcmConfig
						.getSourceConfig().getDefaultConnectionConfig(),
						hostConfigList.get(index++)));

		}
		System.out.println("Total Batch Size : " + totalBatchSize);

		return splits;
	}
}
