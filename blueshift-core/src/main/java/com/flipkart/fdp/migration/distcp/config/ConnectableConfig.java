package com.flipkart.fdp.migration.distcp.config;

import java.util.List;

public interface ConnectableConfig {
	public ConnectionConfig getDefaultConnectionConfig();

	public List<ConnectionConfig> getConnectionConfig();
}
