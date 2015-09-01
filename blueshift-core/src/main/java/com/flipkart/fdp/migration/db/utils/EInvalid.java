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

package com.flipkart.fdp.migration.db.utils;

public class EInvalid extends EBase {
	private static final long serialVersionUID = -3464157895358347518L;

	public EInvalid(Exception e) {
		super(e);
	}

	public EInvalid(String msg) {
		super(msg);
	}

	@Override
	public int httpErrorCode() {
		return 400;
	}
}
