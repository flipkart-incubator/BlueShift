package com.flipkart.fdp.migration.filter;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.vo.FileTuple;

public interface Filter {

	//currently required attributes added it in fileTuple itself, 
	//or we can have generic-params Map here instead of fileTuple.
	public boolean doFilter(DCMConfig dcmConfig, FileTuple fileTuple);
	
}
