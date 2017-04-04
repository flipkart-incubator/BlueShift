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

package com.flipkart.fdp.migration.db.models;

import static com.flipkart.fdp.migration.db.utils.DBUtils.*;
import static javax.persistence.EnumType.STRING;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

import lombok.Getter;

import com.flipkart.fdp.migration.db.utils.DBUtils;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;

@Entity
@Table(name = TAB_BATCH_RUNS)
@Getter
public class BatchRun implements Serializable {

	private static final long serialVersionUID = 4361283698132475133L;

	@Id
	@NotNull
	@Column(name = COL_JOB_ID, updatable = false, nullable = false)
	private String jobId;

	@Column(name = DBUtils.COL_START_TIME, nullable = false)
	private long startTime;

	@Column(name = DBUtils.COL_END_TIME, nullable = false)
	private long endTime;

	@Column(name = DBUtils.COL_BATCH_ID, nullable = false)
	private long batchId;

	@Column(name = COL_BATCH_CONFIG, nullable = false, length = 4096)
	private String batchConfig;

	@Column(name = COL_STATUS)
	@Enumerated(STRING)
	private Status status;

	@Column(name = COL_TRACKING_URL)
	private String trackingURL;

	@Column(name = COL_FAILURE_REASON)
	private String failureReason;

	public BatchRun() {

	}

	public BatchRun(String jobId, long startTime, long endTime, long batchId,
			String batchConfig, String trackingURL, Status status, String reason) {
		this.jobId = jobId;
		this.startTime = startTime;
		this.endTime = endTime;
		this.batchId = batchId;
		this.batchConfig = batchConfig;
		this.status = status;
		this.trackingURL = trackingURL;
		this.failureReason = reason;
	}

}
