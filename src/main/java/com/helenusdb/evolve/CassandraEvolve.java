/*
 * Apache 2 License goes here
 */
package com.helenusdb.evolve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.helenusdb.evolve.metadata.CassandraMetadataStrategy;
import com.helenusdb.evolve.metadata.Metadata;
import com.helenusdb.evolve.metadata.MetadataStrategy;

/**
 * CassandraEvolve is the entry point for managing the migration of a Cassandra
 * database schema. It provides a simple API for migrating the database forward
 * to a specific version.
 * 
 * @see MigrationConfiguration
 * @see Migration
 */
public class CassandraEvolve
{
	private static final Logger LOG = LoggerFactory.getLogger(CassandraEvolve.class);

	/**
	 * Constant to indicate that the metadata table has not been initialized.
	 */
	private static final int UNINITIALIZED = -1;

	/**
	 * Configuration for managing migrations.
	 */
	private MigrationConfiguration configuration;

	/**
	 * Set of migrations to manage.
	 */
	private Set<Migration> migrations = new HashSet<>();

	/**
	 * Strategy for managing the internal metadata that tracks the current
	 * version of the database.
	 * 
	 * @see MetadataStrategy
	 * @see CassandraMetadataStrategy
	 */
	private MetadataStrategy metadata;

	/**
	 * Manage migrations using the default configuration.
	 */
	public CassandraEvolve()
	{
		this(new MigrationConfiguration());
	}

	/**
	 * Manage migrations using the provided configuration.
	 * 
	 * @param configuration The configuration to use for managing migrations.
	 * @see MigrationConfiguration
	 */
	public CassandraEvolve(MigrationConfiguration configuration)
	{
		super();
		setConfiguration(configuration);
	}

	/**
     * Set the configuration to use for managing migrations.
     * 
     * @param configuration The configuration to use for managing migrations.
     * @return this CassandraEvolve instance for method chaining.
     * @see MigrationConfiguration
     */
	public CassandraEvolve setConfiguration(MigrationConfiguration configuration)
	{
		this.configuration = configuration;
		this.metadata = new CassandraMetadataStrategy(configuration);
		return this;
	}

	/**
	 * Get the configuration used for managing migrations.
	 * 
	 * @return The configuration used for managing migrations.
	 * @see MigrationConfiguration
	 */
	public MigrationConfiguration getConfiguration()
	{
		return configuration;
	}

	/**
	 * Register a migration to be managed by this CassandraEvolve instance.
	 * 
	 * @param migration The migration to be managed.
	 * return this CassandraEvolve instance for method chaining.
	 * @see Migration
	 */
	public CassandraEvolve register(Migration migration)
	{
		migrations.add(migration);
		return this;
	}

	/**
     * Register a collection of migrations to be managed by this CassandraEvolve instance.
     * 
     * @param migrations The migrations to be managed.
     * return this CassandraEvolve instance for method chaining.
     * @see Migration
     */
	public CassandraEvolve registerAll(Collection<Migration> migrations)
	{
		this.migrations.addAll(migrations);
		return this;
	}

	/**
	 * Migrate the Cassandra database to the latest version.
	 * 
	 * @param session The Cassandra session to use for executing the migration.
	 * @throws IOException If an error occurs while processing the migration scripts.
	 */
	public void migrate(final CqlSession session)
	throws IOException
	{
		if (!acquireLock(session))
		{
			LOG.info("Data migration in process. Waiting...");
			hold(session);
			LOG.info("Data migration complete. Continuing...");
			return;
		}

		int currentVersion = getCurrentVersion(session);

		if (currentVersion == UNINITIALIZED)
		{
			currentVersion = initializeMetadata(session);
		}

		Collection<Migration> scriptMigrations = discoverMigrationScripts();
		List<Migration> allMigrations = new ArrayList<>(migrations.size() + scriptMigrations.size());
		allMigrations.addAll(migrations);
		allMigrations.addAll(scriptMigrations);
		Collections.sort(allMigrations, Collections.reverseOrder());
		int latestVersion = allMigrations.get(allMigrations.size() -1).getVersion();

		if (currentVersion < latestVersion)
		{
			LOG.info("Database needs migration from current version {} to version {}", currentVersion, latestVersion);

			try
			{
				boolean wasSuccessful = process(session, allMigrations, currentVersion, latestVersion);
	
				if (!wasSuccessful)
				{
					throw new MigrationException("Migration aborted");
				}
	
				LOG.info("Database migration completed successfuly.");
			}
			finally
			{
				releaseLock(session);
			}
		}
	}

	
	private void hold(CqlSession session)
	{
		do
		{
			try
			{
				Thread.sleep(1000l);
			}
			catch (InterruptedException e)
			{
				LOG.warn("Database migration interrupted", e);
				// Restore interrupted state...
			    Thread.currentThread().interrupt();
				return;
			}
		}
		while (metadata.isLocked(session));
	}

	/**
	 * Acquire a lock to prevent concurrent migrations. This method will
	 * write a lock record to the metadata table to indicate that a migration
	 * is in progress. Other clients will be able to detect this lock and
	 * wait for it to be released before proceeding with their own migrations.
	 * 
	 * @param session The Cassandra session to use for acquiring the lock.
	 * @return true if the lock was acquired, false otherwise.
	 */
	private boolean acquireLock(CqlSession session)
	{
		return metadata.acquireLock(session);
	}

	/**
	 * Releases the lock to allow other clients to proceed with their migrations.
	 * 
	 * @param session The Cassandra session to use for releasing the lock.
	 */
	private void releaseLock(CqlSession session)
	{
		metadata.releaseLock(session);
	}

	/**
	 * Process all the migrations from the current version to the latest version.
	 * 
	 * @param session The Cassandra session to use for executing the migrations.
	 * @param allMigrations The collection of all migrations to process.
	 * @param from The current version of the database.
	 * @param to The latest version of the database (being migrated to).
	 * @return true if all migrations were successful, false otherwise.
	 */
	private boolean process(final CqlSession session, final List<Migration> allMigrations, final int from, final int to)
	{
		for (Migration migration : allMigrations)
		{
			if (migration.isApplicable(from, to))
			{
				LOG.info("Migrating database to version {}.", migration.getVersion());
				long startTimeMillis = System.currentTimeMillis();
				boolean wasSuccessful = migration.migrate(session);
				long executionTime = System.currentTimeMillis() - startTimeMillis;
				metadata.update(session, new Metadata(migration, executionTime, wasSuccessful));

				if (!wasSuccessful)
				{
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Discover all the migration scripts available on the classpath.
	 * 
	 * @return A collection of migration scripts.
	 * @throws IOException If an error occurs while loading the migration scripts.
	 */
	private Collection<Migration> discoverMigrationScripts()
	throws IOException
	{
		return new ClasspathMigrationLoader().load(configuration);
	}

	/**
     * Get the current version of the database from the metadata table.
     * 
     * @param session The Cassandra session to use for querying the metadata table.
     * @return The current version of the database
     */
	private int getCurrentVersion(CqlSession session)
	{
		if (!metadata.exists(session))
		{
			return UNINITIALIZED;
		}

		return metadata.getCurrentVersion(session);
	}

	/**
	 * Initialize the metadata table for tracking migrations. The location of the
	 * metadata table is determined by the {@link MigrationConfiguration}. The
	 * metadata table will be created if it does not already exist.
	 * 
	 * @param session The Cassandra session to use for initializing the metadata table.
	 * @return The current version of the database (zero).
	 * @see MigrationConfiguration
	 */
	private int initializeMetadata(CqlSession session)
	{
		metadata.initialize(session);
		return 0;
	}
}
