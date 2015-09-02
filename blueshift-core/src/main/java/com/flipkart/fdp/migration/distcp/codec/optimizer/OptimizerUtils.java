package com.flipkart.fdp.migration.distcp.codec.optimizer;

import java.util.List;
import java.util.Set;

import com.flipkart.fdp.optimizer.OptimTuple;
import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.flipkart.fdp.optimizer.api.JobLoadOptimizerFactory;

public class OptimizerUtils {

	public static List<Set<IInputJob>> optimizeWorkload(
			JobLoadOptimizerFactory.Optimizer optimizer, Set<OptimTuple> tasks,
			int numMappers) {

		System.out.println("Total Tasks: " + tasks.size() + " Total Mappers: "
				+ numMappers);

		System.out.println("Using Optimizer: " + optimizer.toString());
		IJobLoadOptimizer iJobLoadOptimizer = JobLoadOptimizerFactory
				.getJobLoadOptimizerFactory(optimizer);
		List<Set<IInputJob>> optimizedLoadSets = iJobLoadOptimizer
				.getOptimizedLoadSets(tasks, numMappers);
		return optimizedLoadSets;
	}
}
