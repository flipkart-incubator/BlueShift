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

import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_BATCH_DESC;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_BATCH_NAME;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_LAST_RUN_ID;
import static com.flipkart.fdp.migration.db.utils.DBUtils.COL_STATUS;
import static com.flipkart.fdp.migration.db.utils.DBUtils.TAB_BATCH;
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

@Entity
@Table(name = TAB_BATCH)
@Getter
public class Batch implements Serializable {

	private static final long serialVersionUID = -5845417058685772115L;

	@Id
	@Column(name = DBUtils.COL_BATCH_ID, nullable = false, unique = true)
	private long batchId;

	@Column(name = COL_BATCH_NAME, nullable = false)
	private String batchName;

	@Column(name = COL_LAST_RUN_ID, nullable = false)
	private String lastRunJobId;

	@Column(name = COL_BATCH_DESC, nullable = false)
	private String desc;

	@Column(name = COL_STATUS)
	@Enumerated(STRING)
	private Status status;

	@Column(name = COL_BATCH_DESC, nullable = false)
	private boolean lock;

	public Batch() {

	}

	public Batch(long batchId, String batchName, String lastRunJobId,
			String desc, Status status, boolean lock) {
		this.batchId = batchId;
		this.batchName = batchName;
		this.lastRunJobId = lastRunJobId;
		this.desc = desc;
		this.status = status;
		this.lock = lock;
	}

}
