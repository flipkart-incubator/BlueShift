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

package com.flipkart.fdp.optimizer.api;

import com.flipkart.fdp.optimizer.impl.AmbitiousOptimizer;
import com.flipkart.fdp.optimizer.impl.PTASOptimizer;
import com.flipkart.fdp.optimizer.impl.PriorityQueueBasedOptimizer;

/**
 * Created with IntelliJ IDEA. User: ankurg Date: 6/1/15 Time: 8:19 AM To change
 * this template use File | Settings | File Templates.
 */
public class JobLoadOptimizerFactory {
	public static enum Optimizer {
		PRIORITY_QUEUE_BASED, AMBITIOUS, PTASOPTMIZER
	}

	public static IJobLoadOptimizer getJobLoadOptimizerFactory(
			Optimizer optimizer) {
		if (optimizer == Optimizer.PRIORITY_QUEUE_BASED) {
			return new PriorityQueueBasedOptimizer();
		} else if (optimizer == Optimizer.AMBITIOUS) {
			return new AmbitiousOptimizer();
		} else if (optimizer == Optimizer.PTASOPTMIZER) {
			return new PTASOptimizer();
		}
		return null;
	}

}
