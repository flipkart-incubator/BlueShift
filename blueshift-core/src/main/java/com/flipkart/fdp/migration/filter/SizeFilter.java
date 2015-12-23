package com.flipkart.fdp.migration.filter;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.vo.FileTuple;

public class SizeFilter implements Filter {

	@Override
	public boolean doFilter(DCMConfig dcmConfig, FileTuple fileTuple) {
		
		long fileSize = fileTuple.getSize();
		if (fileSize <= 0 && dcmConfig.getSourceConfig().isIgnoreEmptyFiles()) {
			return true;
		}

		if (fileSize < dcmConfig.getSourceConfig().getMinFilesize()) {
			return true;
		}

		return false;
	}

}
