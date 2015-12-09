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
import java.util.PriorityQueue;
import java.util.Set;

import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;

public class PriorityQueueBasedOptimizer implements IJobLoadOptimizer {

	public List<Set<IInputJob>> getOptimizedLoadSets(
			Set<? extends IInputJob> jobs, int noOfWorkers) {
		final PriorityQueue<JobSet> jobSets = new PriorityQueueBasedImpl(jobs,
				noOfWorkers).execute();
		return getJobSets(jobSets);
	}

	private static List<Set<IInputJob>> getJobSets(PriorityQueue<JobSet> jobSets) {
		List<Set<IInputJob>> jobLists = new ArrayList<Set<IInputJob>>();
		for (JobSet jobSet : jobSets) {
			jobLists.add(jobSet.jobSet);
		}
		return jobLists;
	}

	private static class PriorityQueueBasedImpl {
		private final Set<? extends IInputJob> jobs;
		private final int noOfWorkers;

		PriorityQueueBasedImpl(Set<? extends IInputJob> jobs, int noOfWorkers) {
			this.jobs = jobs;
			this.noOfWorkers = noOfWorkers;
		}

		PriorityQueue<JobSet> execute() {
			List<IInputJob> sortedJobs = getReverseSortedInputJobs();
			PriorityQueue<JobSet> minHeap = new PriorityQueue<JobSet>(
					noOfWorkers);
			fillInitialJobSets(sortedJobs, minHeap);
			assignRestOfJobs(sortedJobs, minHeap);
			return minHeap;
		}

		private void assignRestOfJobs(List<IInputJob> sortedJobs,
				PriorityQueue<JobSet> minHeap) {
			for (IInputJob job : sortedJobs) {
				JobSet minJobSet = minHeap.poll();

				if (minJobSet != null) {
					minJobSet.addJob(job);
				}
				minHeap.add(minJobSet);
			}
		}

		private void fillInitialJobSets(List<IInputJob> sortedJobs,
				PriorityQueue<JobSet> minHeap) {
			for (int counter = 0; counter < noOfWorkers
					&& sortedJobs.size() > 0; counter++) {
				IInputJob job = sortedJobs.remove(0);
				JobSet jobSet = new JobSet().addJob(job);
				minHeap.add(jobSet);
			}
		}

		private List<IInputJob> getReverseSortedInputJobs() {
			List<IInputJob> sortedJobs = new ArrayList<IInputJob>(jobs);
			Collections.sort(sortedJobs);
			Collections.reverse(sortedJobs);
			return sortedJobs;
		}
	}

	private static class JobSet implements Comparable<JobSet> {
		//TODO hashcode and equals to be overridden?
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
