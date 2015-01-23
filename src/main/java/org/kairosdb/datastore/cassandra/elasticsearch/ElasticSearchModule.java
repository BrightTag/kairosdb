package org.kairosdb.datastore.cassandra.elasticsearch;

import javax.inject.Named;

import com.google.common.base.Splitter;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.kairosdb.datastore.cassandra.CassandraConfiguration;

/**
 * @author codyaray
 * @since 1/20/15
 */
public class ElasticSearchModule extends AbstractModule {
  public static final String INDEX_NAME = "elasticsearch.index_name";

  @Override
  protected void configure() {
    bind(ElasticSearchRowKeyPlugin.class);
    bind(ElasticSearchRowKeyListener.class);
  }

  @Provides @Named(INDEX_NAME)
  String provideElasticSearchIndexName(CassandraConfiguration configuration) {
    return configuration.getElasticSearchIndexName();
  }

  private static final Splitter HOST_LIST_SPLITTER = Splitter.on(',').trimResults();

  @Provides @Singleton
  Client provideElasticSearchHostList(CassandraConfiguration configuration) {
    TransportClient client = new TransportClient(ImmutableSettings.settingsBuilder()
        .put("cluster.name", configuration.getElasticSearchClusterName()));
    for (String host : HOST_LIST_SPLITTER.split(configuration.getElasticSearchHostList())) {
      client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
    }
    return client;
  }
}
