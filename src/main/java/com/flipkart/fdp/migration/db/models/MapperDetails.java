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

import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_FILE_PATH;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_STATUS;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_TASK_ID;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_DIGEST;
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

@Entity
@Table(name = TAB_MAPPER_DETAILS)
@Getter
public class MapperDetails implements Serializable {

	private static final long serialVersionUID = 3814671000501339348L;

	@Id
	@Column(name = DBUtils.COL_BATCH_ID, updatable = false, nullable = false)
	private long batchId;

	@Id
	@Column(name = COL_TASK_ID, updatable = false, nullable = false)
	private String taskId;

	@Column(name = COL_FILE_PATH, nullable = false, length = 1024)
	private String filePath;

	@Column(name = COL_DIGEST, nullable = false)
	private String digest;

	@Column(name = COL_STATUS, nullable = false)
	@Enumerated(STRING)
	private Status status;

	public MapperDetails() {

	}

	public MapperDetails(long batchId, String filePath, Status status,
			String digest, String taskId) {
		this.batchId = batchId;
		this.filePath = filePath;
		this.status = status;
		this.digest = digest;
		this.taskId = taskId;
	}

}
