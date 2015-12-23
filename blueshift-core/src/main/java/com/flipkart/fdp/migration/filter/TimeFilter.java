package com.flipkart.fdp.migration.filter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.flipkart.fdp.migration.distcp.config.DCMConfig;
import com.flipkart.fdp.migration.vo.FileTuple;

public class TimeFilter implements Filter {

	private String pattern = "yyyy-MM-dd HH:mm:ss"; 
	private SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	
	@Override
	public boolean doFilter(DCMConfig dcmConfig, FileTuple fileTuple) {
		
		String startTime = dcmConfig.getSourceConfig().getStartTime();
		long ts = getTimeStamp(startTime);
		if (ts > 0 && fileTuple.getTs() < ts) {
			return true;
		}

		String endTime = dcmConfig.getSourceConfig().getEndTime();
		ts = getTimeStamp(endTime);
		if (ts > 0 && fileTuple.getTs() > ts) {
			return true;
		}
		
		return false;
	}
	
	//TODO move this computation once during startup 
	private long getTimeStamp(String time) {
		try {
			Date date = sdf.parse(time);
			return date.getTime();
		} catch (ParseException e) {
			System.err.println("Bad time config passed : " + time + "\t Expected format : " + pattern);
		}
		return 0;
	}

}
