package com.helenusdb.evolve;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * @author tfredrich
 * @since May 8, 2015
 */
public class CassandraManager
{
	private static final CassandraManager INSTANCE = new CassandraManager();
	private static final String LOCALHOST = "127.0.0.1";
	private static final String KEYSPACE_NAME = "helenusdb_evolve_test";

	private boolean isStarted;
	private Cluster cluster;
	private CqlSession session;
	private Metadata metadata;

	public static void start()
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		start(true);
	}

	public static void start(boolean isEmbedded)
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		INSTANCE._start(isEmbedded);
	}

	public static Cluster cluster()
    {
	    return INSTANCE._cluster();
    }

	public static CqlSession session()
	{
		return INSTANCE._session();
	}

	public static String keyspace()
	{
		return KEYSPACE_NAME;
	}

	public static Metadata metadata()
	{
		return INSTANCE._metadata();
	}

	private Cluster _cluster()
	{
		if (isStarted)
		{
			return cluster;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing cluster");		
	}

	private CqlSession _session()
	{
		if (isStarted)
		{
			return session;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing session");
	}

	private Metadata _metadata()
	{
		if (isStarted)
		{
			return metadata;
		}

		throw new IllegalStateException("Call CassandraManager.start() before accessing metadata");
	}

	private synchronized void _start(boolean isEmbedded)
	throws ConfigurationException, TTransportException, IOException, InterruptedException
	{
		if (isStarted) return;

		if (isEmbedded)
		{
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			cluster = Cluster.builder()
				.addContactPoints(LOCALHOST)
				.withPort(9142)
				.build();
		}
		else
		{
			cluster = Cluster.builder()
				.addContactPoints(LOCALHOST)
				.build();
		}

		session = (CqlSession) cluster.connect();
		metadata = cluster.getMetadata();
		initializeKeyspace(session);
		isStarted = true;
	}

	private void initializeKeyspace(CqlSession session)
	{
		session.execute(String.format("create keyspace if not exists %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }", keyspace()));
	}
}
