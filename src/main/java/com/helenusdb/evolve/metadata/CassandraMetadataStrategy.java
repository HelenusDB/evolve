package com.helenusdb.evolve.metadata;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.helenusdb.evolve.MigrationConfiguration;
import com.helenusdb.evolve.MigrationException;

public class CassandraMetadataStrategy
implements MetadataStrategy
{
	private static final String MIGRATIONS_KEY = "migrations";

	private MigrationConfiguration config;
	private PreparedStatement updateStatement;
	private PreparedStatement lockCheckStatement;

	public CassandraMetadataStrategy(MigrationConfiguration configuration)
	{
		super();
		this.config = configuration;
	}

	public boolean exists(CqlSession session)
	{
		ResultSet rs = session.execute(String.format("select count(*) from system_schema.tables where keyspace_name='%s' and table_name='%s'", session.getKeyspace(), config.getMetadataTable()));
		return (rs.one().getLong(0) > 0);
	}

	public int getCurrentVersion(CqlSession session)
	{
		ResultSet rs = session.execute(String.format("select version from %s.%s where name = %s limit 1", session.getKeyspace(), config.getMetadataTable(), MIGRATIONS_KEY));
		return rs.one().getInt(0);
	}

	public void initialize(CqlSession session)
	{
		ResultSet rs = session.execute(String.format("create table if not exists %s.%s (" +
			"name text," +
			"version int," +
			"description text," +
			"script text," +
			"hash text," +
			"installed_on timestamp," +
			"exectime_ms bigint," +
			"was_successful boolean," +
			"primary key ((name), installed_on, version)" +
		") with clustering order by (installed_on DESC, version DESC);",
		config.getKeyspace(), config.getMetadataTable()));

		if (!rs.wasApplied())
		{
			throw new MigrationException("Migration metadata intialization failed");
		}

		rs = session.execute(String.format("create table if not exists %s.%s_lock (" +
				"name text," +
				"locked_at timestamp," +
				"primary key (name)" +
			");",
			config.getKeyspace(), config.getMetadataTable()));

		if (!rs.wasApplied())
		{
			throw new MigrationException("Migration metadata lock table intialization failed");
		}
	}

	public void update(CqlSession session, Metadata metadata)
	{
		if (updateStatement == null)
		{
			updateStatement = session.prepare(String.format("insert into %s.%s (name, version, description, script, hash, installed_on, exectime_ms, was_successful) values (?, ?, ?, ?, ?, ?, ?, ?)",
					config.getKeyspace(), config.getMetadataTable()));
		}

		ResultSet rs = session.execute(updateStatement.bind(MIGRATIONS_KEY, metadata.getVersion(), metadata.getDescription(), metadata.getScript(), metadata.getHash(), metadata.getInstalledAt(), metadata.getExecutionTime(), metadata.isWasSuccessful()));

		if (!rs.wasApplied())
		{
			throw new MigrationException("Failed to update migration metadata.");
		}
	}

	public boolean acquireLock(CqlSession session)
	{
		PreparedStatement ps = session.prepare(String.format("insert into %s.%s_lock (name, locked_at) values (?, ?) if not exists",
			config.getKeyspace(), config.getMetadataTable()));
		ResultSet rs = session.execute(ps.bind(MIGRATIONS_KEY, System.currentTimeMillis()));

		return rs.wasApplied();
	}

	public boolean isLocked(CqlSession session)
	{
		if (this.lockCheckStatement == null)
		{
			this.lockCheckStatement = session.prepare(String.format("select count(*) from %s.%s_lock where name = ?", config.getKeyspace(), config.getMetadataTable()));
		}

		ResultSet rs = session.execute(lockCheckStatement.bind(MIGRATIONS_KEY));
		Row row = rs.one();

		if (row == null) return false;
		return (row.getInt(0) > 0);
	}

	public void releaseLock(CqlSession session)
	{
		PreparedStatement ps = session.prepare(String.format("delete from %s.%s_lock where name = ? if exists",
			config.getKeyspace(), config.getMetadataTable()));
		ResultSet rs = session.execute(ps.bind(MIGRATIONS_KEY));


		if (!rs.wasApplied())
		{
			throw new MigrationException("Failed to release migration metadata lock.");
		}
	}
}
