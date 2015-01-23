package org.kairosdb.datastore.cassandra;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;

/**
 Created by bhawkins on 10/13/14.
 */
public class CassandraConfiguration
{
	public static final String READ_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.read_consistency_level";
	public static final String WRITE_CONSISTENCY_LEVEL = "kairosdb.datastore.cassandra.write_consistency_level";
	public static final String DATAPOINT_TTL = "kairosdb.datastore.cassandra.datapoint_ttl";

	public static final String ROW_KEY_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.row_key_cache_size";
	public static final String STRING_CACHE_SIZE_PROPERTY = "kairosdb.datastore.cassandra.string_cache_size";

	public static final String KEYSPACE_PROPERTY = "kairosdb.datastore.cassandra.keyspace";
	public static final String REPLICATION_FACTOR_PROPERTY = "kairosdb.datastore.cassandra.replication_factor";
	public static final String WRITE_DELAY_PROPERTY = "kairosdb.datastore.cassandra.write_delay";

	public static final String WRITE_BUFFER_SIZE = "kairosdb.datastore.cassandra.write_buffer_max_size";
	public static final String SINGLE_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.single_row_read_size";
	public static final String MULTI_ROW_READ_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_read_size";
	public static final String MULTI_ROW_SIZE_PROPERTY = "kairosdb.datastore.cassandra.multi_row_size";

	public static final String ELASTICSEARCH_HOST_LIST = "kairosdb.datastore.cassandra.elasticsearch.host_list";
	public static final String ELASTICSEARCH_CLUSTER_NAME = "kairosdb.datastore.cassandra.elasticsearch.cluster_name";
	public static final String ELASTICSEARCH_INDEX_NAME = "kairosdb.datastore.cassandra.elasticsearch.index_name";

	@Inject
	@Named(WRITE_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataWriteLevel = ConsistencyLevel.QUORUM;

	@Inject
	@Named(READ_CONSISTENCY_LEVEL)
	private ConsistencyLevel m_dataReadLevel = ConsistencyLevel.ONE;

	@Inject(optional=true)
	@Named(DATAPOINT_TTL)
	private int m_datapointTtl = 0; //Zero ttl means data lives forever.

	@Inject
	@Named(ROW_KEY_CACHE_SIZE_PROPERTY)
	private int m_rowKeyCacheSize = 1024;

	@Inject
	@Named(STRING_CACHE_SIZE_PROPERTY)
	private int m_stringCacheSize = 1024;

	@Inject
	@Named(CassandraModule.CASSANDRA_AUTH_MAP)
	private Map<String, String> m_cassandraAuthentication;

	@Inject
	@Named(REPLICATION_FACTOR_PROPERTY)
	private int m_replicationFactor;

	@Inject
	@Named(SINGLE_ROW_READ_SIZE_PROPERTY)
	private int m_singleRowReadSize;

	@Inject
	@Named(MULTI_ROW_SIZE_PROPERTY)
	private int m_multiRowSize;

	@Inject
	@Named(MULTI_ROW_READ_SIZE_PROPERTY)
	private int m_multiRowReadSize;

	@Inject
	@Named(WRITE_DELAY_PROPERTY)
	private int m_writeDelay;

	@Inject
	@Named(WRITE_BUFFER_SIZE)
	private int m_maxWriteSize;

	@Inject
	@Named(KEYSPACE_PROPERTY)
	private String m_keyspaceName;

	@Inject
	@Named(ELASTICSEARCH_HOST_LIST)
	private String m_elasticSearchHostList;

	@Inject(optional=true)
	@Named(ELASTICSEARCH_CLUSTER_NAME)
	private String m_elasticSearchClusterName = "elasticsearch";

	@Inject(optional=true)
	@Named(ELASTICSEARCH_INDEX_NAME)
	private String m_elasticSearchIndexName = "kairosdb";

	public CassandraConfiguration()
	{
	}

	public CassandraConfiguration(int replicationFactor,
			int singleRowReadSize,
			int multiRowSize,
			int multiRowReadSize,
			int writeDelay,
			int maxWriteSize,
			String keyspaceName,
			String elasticSearchHostList,
			String elasticSearchClusterName,
			String elasticSearchIndexName)
	{
		m_replicationFactor = replicationFactor;
		m_singleRowReadSize = singleRowReadSize;
		m_multiRowSize = multiRowSize;
		m_multiRowReadSize = multiRowReadSize;
		m_writeDelay = writeDelay;
		m_maxWriteSize = maxWriteSize;
		m_keyspaceName = keyspaceName;
		m_elasticSearchHostList = elasticSearchHostList;
		m_elasticSearchClusterName = elasticSearchClusterName;
		m_elasticSearchIndexName = elasticSearchIndexName;
	}

	public ConsistencyLevel getDataWriteLevel()
	{
		return m_dataWriteLevel;
	}

	public ConsistencyLevel getDataReadLevel()
	{
		return m_dataReadLevel;
	}

	public int getDatapointTtl()
	{
		return m_datapointTtl;
	}

	public int getRowKeyCacheSize()
	{
		return m_rowKeyCacheSize;
	}

	public int getStringCacheSize()
	{
		return m_stringCacheSize;
	}

	public Map<String, String> getCassandraAuthentication()
	{
		return m_cassandraAuthentication;
	}

	public int getReplicationFactor()
	{
		return m_replicationFactor;
	}

	public int getSingleRowReadSize()
	{
		return m_singleRowReadSize;
	}

	public int getMultiRowSize()
	{
		return m_multiRowSize;
	}

	public int getMultiRowReadSize()
	{
		return m_multiRowReadSize;
	}

	public int getWriteDelay()
	{
		return m_writeDelay;
	}

	public int getMaxWriteSize()
	{
		return m_maxWriteSize;
	}

	public String getKeyspaceName()
	{
		return m_keyspaceName;
	}

	public String getElasticSearchHostList() {
		return m_elasticSearchHostList;
	}

	public String getElasticSearchClusterName() {
		return m_elasticSearchClusterName;
	}

	public String getElasticSearchIndexName() {
		return m_elasticSearchIndexName;
	}
}
