package com.helenusdb.evolve.metadata;

import com.datastax.oss.driver.api.core.CqlSession;

public interface MetadataStrategy
{
	public boolean exists(CqlSession session);
	public int getCurrentVersion(CqlSession session);
	public void initialize(CqlSession session);
	public void update(CqlSession session, Metadata metadata);
	public boolean acquireLock(CqlSession session);
	public boolean isLocked(CqlSession session);
	public void releaseLock(CqlSession session);
}
