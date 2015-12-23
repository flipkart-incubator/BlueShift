package com.flipkart.fdp.migration.filter;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.vo.FileTuple;

public class StateFilter implements Filter {
	
	@Override
	public boolean doFilter(DCMConfig dcmConfig, FileTuple fileTuple) {
		
//		String path = fileTuple.getFileName();
//		if (previousState.containsKey(path)) {
//			TransferStatus details = previousState.get(path);
//			if (details.getStatus() == Status.COMPLETED) {
//				if (dcmConfig.getSourceConfig().isIncludeUpdatedFiles() && details.getTs() < fileTuple.getTs()) {
//					return false;
//				}
//				return true;
//			}
//		}
		return false;
	}

}
