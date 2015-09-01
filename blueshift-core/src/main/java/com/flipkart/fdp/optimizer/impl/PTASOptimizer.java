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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.flipkart.fdp.optimizer.api.IInputJob;
import com.flipkart.fdp.optimizer.api.IJobLoadOptimizer;

public class PTASOptimizer implements IJobLoadOptimizer {

	// this is used to get the basic buckets to start with
	IJobLoadOptimizer optimizer = new AmbitiousOptimizer();

	class Bucket {
		SortedSet<IInputJob> inputJobs;
		long size;

		public Bucket(SortedSet<IInputJob> objects, long size) {
			inputJobs = objects;
			this.size = size;
		}
	}

	public List<Set<IInputJob>> getOptimizedLoadSets(
			Set<? extends IInputJob> jobs, int noOfWorkers) {
		List<Set<IInputJob>> basicBuckets = optimizer.getOptimizedLoadSets(
				jobs, noOfWorkers);
		long sum = 0;
		SortedSet<Bucket> copySet = new TreeSet<Bucket>(
				new Comparator<Bucket>() {
					public int compare(Bucket o1, Bucket o2) {
						if (o1.size == o2.size) {
							return -1;
						}
						return (-1) * (int) (o1.size - o2.size);
					}
				});
		for (Set<IInputJob> basicJobs : basicBuckets) {
			long size = 0;

			for (IInputJob job : basicJobs) {
				size += job.getJobSize();
			}
			TreeSet<IInputJob> set = new TreeSet<IInputJob>(
					new Comparator<IInputJob>() {
						public int compare(IInputJob o1, IInputJob o2) {
							if (o1.getJobSize() == o2.getJobSize()) {
								if (o1.equals(o2)) {
									return 0;
								} else {
									return -1;
								}
							}
							return o1.compareTo(o2);
						}
					});
			set.addAll(basicJobs);
			copySet.add(new Bucket(set, size));
			sum += size;
		}

		final long average = sum / basicBuckets.size();

		SortedSet<Bucket> set = new TreeSet<Bucket>(new Comparator<Bucket>() {
			public int compare(Bucket o1, Bucket o2) {
				if (o1.equals(o2)) {
					return 0;
				}
				if (o1.size > average && o2.size > average) {
					int i = 0;
					int j = 0;

					while (i < o1.inputJobs.size() && j < o2.inputJobs.size()) {
						Object[] o1Array = o1.inputJobs.toArray();
						Object[] o2Array = o2.inputJobs.toArray();
						if (((IInputJob) o1Array[i]).getJobSize() > ((IInputJob) o2Array[j])
								.getJobSize()) {
							return -1;
						} else if (((IInputJob) o1Array[i]).getJobSize() < ((IInputJob) o2Array[j])
								.getJobSize()) {
							return 1;
						} else {
							i++;
							j++;
						}
					}
				} else {
					if (o1.size == o2.size) {

						if (o1.equals(o2)) {
							return 0;
						} else {
							return -1;
						}
					}

					return (-1) * (int) (o1.size - o2.size);
				}
				return -1;
			}
		});

		set.addAll(copySet);

		List<Set<IInputJob>> optimizedBuckets = new ArrayList<Set<IInputJob>>();

		while (true) {
			Bucket bucket = set.first();
			if (bucket.size < average || set.last().size == average) {
				break;
			}
			if (bucket.size - bucket.inputJobs.first().getJobSize() < average) {
				optimizedBuckets.add(bucket.inputJobs);
				set.remove(bucket);
			} else {
				set.last().inputJobs.add(bucket.inputJobs.first());
				set.last().size += bucket.inputJobs.first().getJobSize();
				set.first().inputJobs.remove(bucket.inputJobs.first());
				set.first().size -= bucket.inputJobs.first().getJobSize();
			}
		}
		for (Bucket b : set) {
			optimizedBuckets.add(b.inputJobs);
		}
		return optimizedBuckets;
	}
}
