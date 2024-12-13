package com.helenusdb.evolve;

import com.datastax.oss.driver.api.core.CqlSession;

public interface Migration
extends Comparable<Migration>
{
	public int getVersion();
	public String getDescription();
	public boolean isApplicable(int from, int to);
	public boolean migrate(CqlSession session)
	throws MigrationException;
}
