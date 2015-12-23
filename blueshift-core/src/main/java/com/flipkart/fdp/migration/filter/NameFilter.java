package com.flipkart.fdp.migration.filter;

import java.util.Set;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.distcp.core.MirrorUtils;
import com.flipkart.fdp.migration.vo.FileTuple;

public class NameFilter implements Filter {

	@Override
	public boolean doFilter(DCMConfig dcmConfig, FileTuple fileTuple) {
		
		String path = fileTuple.getFileName();
		String excludeListFile = dcmConfig.getSourceConfig().getExcludeListFile();
		Set<String> excludeList = MirrorUtils.getFileAsLists(excludeListFile);
		if (excludeList.contains(path)) {
			return true;
		}
		return false;
	}

}
