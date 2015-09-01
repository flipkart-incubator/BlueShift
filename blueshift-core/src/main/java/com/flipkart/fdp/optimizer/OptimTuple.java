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

package com.flipkart.fdp.optimizer;

import com.flipkart.fdp.optimizer.api.IInputJob;

public class OptimTuple implements IInputJob {

	public final String jobKey;
	public final long size;

	public OptimTuple(String commaSepratedInput) {
		String[] inputArray = commaSepratedInput.split(",");
		this.jobKey = inputArray[0];
		this.size = Integer.valueOf(inputArray[2]);
	}

	public OptimTuple(String jobKey, long size) {
		this.jobKey = jobKey;
		this.size = size;
	}

	public int compareTo(IInputJob o) {
		Long l = getJobSize();
		if (o == null)
			return l.compareTo(0L);
		return l.compareTo(o.getJobSize());
	}

	public long getJobSize() {
		return size;
	}

	public String getJobKey() {
		return jobKey;
	}

	public String toString() {
		return "ReduceDataSet{" + "jobKey='" + jobKey + ", size=" + size + '}';
	}
}
