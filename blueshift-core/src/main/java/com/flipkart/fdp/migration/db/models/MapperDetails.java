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

import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_DIGEST;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_STATUS;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_TASK_ID;
import static com.flipkart.fdp.migration.db.utils.DBUtils.TAB_MAPPER_DETAILS;
import static javax.persistence.EnumType.STRING;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;

import com.flipkart.fdp.migration.db.utils.DBUtils;
import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;
import com.flipkart.fdp.migration.distcp.state.TransferStatus;

@Entity
@Table(name = TAB_MAPPER_DETAILS)
@Getter
public class MapperDetails implements Serializable {

	private static final long serialVersionUID = 3814671000501339348L;

	@Id
	@Column(name = DBUtils.COL_BATCH_ID, updatable = false, nullable = false)
	private long batchId;

	@Id
	@Column(name = DBUtils.COL_SRC_PATH, updatable = false, nullable = false, length = 1024)
	private String srcPath;

	@Id
	@Column(name = COL_TASK_ID, updatable = false, nullable = false)
	private String taskId;

	@Column(name = DBUtils.COL_DEST_PATH, updatable = false, length = 1024)
	private String destPath;

	@Column(name = DBUtils.COL_SRC_SIZE, nullable = false)
	private long srcSize = 0;

	@Column(name = DBUtils.COL_DEST_SIZE, nullable = false)
	private long destSize = 0;

	@Column(name = COL_STATUS, nullable = false)
	@Enumerated(STRING)
	private Status status;

	@Column(name = COL_DIGEST)
	private String digest;

	@Column(name = DBUtils.COL_TS, nullable = false)
	private long ts;

	public MapperDetails() {

	}

	public MapperDetails(long batchId, TransferStatus tstat) {
		this.batchId = batchId;
		this.srcPath = tstat.getInputPath();
		this.destPath = tstat.getOutputPath();
		this.srcSize = tstat.getInputSize();
		this.destSize = tstat.getOutputSize();
		this.status = tstat.getStatus();
		this.digest = tstat.getMd5Digest();
		this.taskId = tstat.getTaskID();
		this.ts = tstat.getTs();
	}

}
