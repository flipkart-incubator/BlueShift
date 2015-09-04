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

package com.flipkart.fdp.migration.distcp.state;

import com.flipkart.fdp.migration.distcp.config.DCMConstants.Status;
import com.google.gson.Gson;

public class TransferStatus {

	private String inputPath = null;
	private String outputPath = null;
	private long inputSize = 0;
	private long outputSize = 0;
	private long ts = 0;
	private boolean inputCompressed = false;
	private boolean inputTransformed = false;
	private boolean outputCompressed = false;
	private String md5Digest = null;
	private String taskID = null;
	private Status status = Status.NEW;

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public boolean isInputCompressed() {
		return inputCompressed;
	}

	public void setInputCompressed(boolean inputCompressed) {
		this.inputCompressed = inputCompressed;
	}

	public boolean isInputTransformed() {
		return inputTransformed;
	}

	public void setInputTransformed(boolean inputTransformed) {
		this.inputTransformed = inputTransformed;
	}

	public boolean isOutputCompressed() {
		return outputCompressed;
	}

	public void setOutputCompressed(boolean outputCompressed) {
		this.outputCompressed = outputCompressed;
	}

	public String getMd5Digest() {
		return md5Digest;
	}

	public void setMd5Digest(String md5Digest) {
		this.md5Digest = md5Digest;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public String getTaskID() {
		return taskID;
	}

	public void setTaskID(String taskID) {
		this.taskID = taskID;
	}

	public long getInputSize() {
		return inputSize;
	}

	public void setInputSize(long inputSize) {
		this.inputSize = inputSize;
	}

	public long getOutputSize() {
		return outputSize;
	}

	public void setOutputSize(long outputSize) {
		this.outputSize = outputSize;
	}

	@Override
	public String toString() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}
}
