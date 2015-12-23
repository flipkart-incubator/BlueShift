package com.flipkart.fdp.migration.filter;

public enum FilterType {
	
	TIME (new TimeFilter()),
	SIZE (new SizeFilter()),
	NAME (new NameFilter());
	
	private Filter filter;

	private FilterType(Filter filter) {
		this.filter = filter;
	}
	
	public Filter getFilter() {
		return filter;
	}
	
}
