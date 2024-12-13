package com.helenusdb.evolve;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

public class MigrationMasterTest
{
	@BeforeClass
	public static void beforeClass()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		CassandraManager.start();
	}

	@Test
	public void testMigrate()
	throws IOException
	{
		CassandraEvolve evolve = new CassandraEvolve();

		// manipulate the migration configuration to use the same keyspace as the CassandraManager (otherwise, we'd have to create a new 'migrations' keyspace).
		MigrationConfiguration config = evolve.getConfiguration();
		config.setKeyspace(CassandraManager.keyspace());
		evolve.setConfiguration(config);

		// perform the migration.
		evolve.migrate(CassandraManager.session());
		assertTrue(true);
	}
}
