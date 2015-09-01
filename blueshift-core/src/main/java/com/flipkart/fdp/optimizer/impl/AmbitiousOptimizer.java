/*
 *
 *  Copyright 2015 Flipkart Internet Pvt. Ltd.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.flipkart.fdp.optimizer.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class AmbitiousOptimizer implements IJobLoadOptimizer {

	public List<Set<IInputJob>> getOptimizedLoadSets(
			Set<? extends IInputJob> jobs, int noOfWorkers) {
		final List<JobSet> jobSets = new AmbitiousImpl(jobs, noOfWorkers)
				.execute();
		return getJobSets(jobSets);
	}

	private static List<Set<IInputJob>> getJobSets(List<JobSet> jobSets) {
		List<Set<IInputJob>> jobLists = new ArrayList<Set<IInputJob>>();
		for (JobSet jobSet : jobSets) {
			jobLists.add(jobSet.jobSet);
		}
		return jobLists;
	}

	private static class AmbitiousImpl {
		private final Set<? extends IInputJob> jobs;
		private final int numberOfProcessors;
		private List<IInputJob> blocks;
		private Map<Integer, List<IInputJob>> resultAllocation;
		private Map<Integer, Long> resultAllocationSum;

		AmbitiousImpl(Set<? extends IInputJob> jobs, int noOfWorkers) {
			this.jobs = jobs;
			this.numberOfProcessors = noOfWorkers;
			resultAllocation = Maps.newHashMap();
			resultAllocationSum = Maps.newHashMap();
		}

		List<JobSet> execute() {
			List<IInputJob> sortedJobs = getReverseSortedInputJobs();
			blocks = new ArrayList<IInputJob>(sortedJobs);
			List<JobSet> jobSets = Lists.newArrayList();

			doAllocation();

			for (Map.Entry<Integer, List<IInputJob>> entry : resultAllocation
					.entrySet()) {
				List<IInputJob> jobs = entry.getValue();
				JobSet jobSet = new JobSet();
				for (IInputJob job : jobs) {
					jobSet.addJob(job);
				}
				jobSets.add(jobSet);
			}

			return jobSets;
		}

		private void doAllocation() {
			int currentProcessor = 0;
			while (blocks.size() > 0) {
				if (currentProcessor >= numberOfProcessors) {
					fillRemainingBlocks();
					break;
				}
				Long average = getAverage(currentProcessor);
				currentProcessor = pruneBiggerBlocks(currentProcessor, average);
				currentProcessor = fillCurrentProcessor(currentProcessor,
						average);
			}
		}

		private int fillCurrentProcessor(int currentProcessor, Long average) {
			if (currentProcessor >= numberOfProcessors) {
				return currentProcessor;
			}
			List<IInputJob> newList = Lists.newArrayList();
			List<IInputJob> subList = Lists.newArrayList();
			Long sum = 0l;
			Long remaining = average;
			for (IInputJob job : blocks) {
				Long num = job.getJobSize();
				if (num <= remaining) {
					subList.add(job);
					sum += num;
					remaining -= num;
				} else {
					newList.add(job);
				}
			}
			resultAllocation.put(currentProcessor, subList);
			resultAllocationSum.put(currentProcessor, sum);
			blocks = newList;
			return currentProcessor + 1;
		}

		private void fillRemainingBlocks() {
			for (IInputJob job : blocks) {
				int index = getMinIndex();
				List<IInputJob> list = resultAllocation.get(index);
				list.add(job);
				resultAllocation.put(index, list);
				resultAllocationSum.put(index, resultAllocationSum.get(index)
						+ job.getJobSize());
			}
		}

		private int getMinIndex() {
			Long min = Long.MAX_VALUE;
			int index = -1;
			for (Map.Entry<Integer, Long> entry : resultAllocationSum
					.entrySet()) {
				if (entry.getValue() < min) {
					min = entry.getValue();
					index = entry.getKey();
				}
			}
			return index;
		}

		private int pruneBiggerBlocks(int currentProcessor, Long average) {
			List<IInputJob> newList = Lists.newArrayList();
			for (IInputJob job : blocks) {
				if (job.getJobSize() > average
						&& currentProcessor < numberOfProcessors) {
					resultAllocation.put(currentProcessor,
							Lists.newArrayList(job));
					resultAllocationSum.put(currentProcessor, job.getJobSize());
					currentProcessor++;
				} else {
					newList.add(job);
				}
			}
			blocks = newList;
			return currentProcessor;
		}

		private Long getAverage(int currentProcessor) {
			Long sum = 0l;
			int numProcessors = numberOfProcessors - currentProcessor;
			for (IInputJob job : blocks) {
				sum += job.getJobSize();
			}
			return sum / numProcessors;
		}

		private List<IInputJob> getReverseSortedInputJobs() {
			List<IInputJob> sortedJobs = new ArrayList<IInputJob>(jobs);
			Collections.sort(sortedJobs);
			Collections.reverse(sortedJobs);
			return sortedJobs;
		}
	}

	private static class JobSet implements Comparable<JobSet> {
		private final Set<IInputJob> jobSet = new HashSet<IInputJob>();
		private BigInteger currentJobSetSize = new BigInteger("0");

		public JobSet addJob(IInputJob job) {
			jobSet.add(job);
			currentJobSetSize = currentJobSetSize.add(new BigInteger(String
					.valueOf(job.getJobSize())));
			return this;
		}

		public BigInteger getCurrentJobSetSize() {
			return currentJobSetSize;
		}

		@Override
		public String toString() {
			return "JobSet [jobSet=" + jobSet + ", currentJobSetSize="
					+ currentJobSetSize + "]";
		}

		public int compareTo(JobSet o) {
			return getCurrentJobSetSize().compareTo(o.getCurrentJobSetSize());
		}
	}

}
