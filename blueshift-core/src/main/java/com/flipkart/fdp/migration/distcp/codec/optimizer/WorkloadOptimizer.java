package com.flipkart.fdp.migration.distcp.codec.optimizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.vo.FileTuple;
import com.flipkart.fdp.optimizer.OptimTuple;

public interface WorkloadOptimizer {

	public List<InputSplit> optimizeWorkload(DCMConfig dcmConfig,
			Set<OptimTuple> locations, HashMap<String, FileTuple> inputFileMap)
			throws IOException;
}
