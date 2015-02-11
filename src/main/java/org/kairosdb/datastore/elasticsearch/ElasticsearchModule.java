package org.kairosdb.datastore.elasticsearch;

import javax.inject.Named;

import com.google.common.base.Splitter;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.kairosdb.core.datastore.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author codyaray
 * @since 2/12/15
 */
public class ElasticsearchModule extends AbstractModule {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchModule.class);

  private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.elasticsearch.host_list";
  private static final String CLUSTER_NAME_PROPERTY = "kairosdb.datastore.elasticsearch.cluster_name";

  @Override
  protected void configure() {
    bind(Datastore.class).to(ElasticsearchDatastore.class).in(Singleton.class);
  }

  private static final Splitter HOST_LIST_SPLITTER = Splitter.on(',').trimResults();

  @Provides
  Client provideElasticsearchClient(@Named(HOST_LIST_PROPERTY) String hostList,
      @Named(CLUSTER_NAME_PROPERTY) String clusterName) {
    logger.info("Using elasticsearch hosts '{}' and cluster name '{}'", hostList, clusterName);
    TransportClient client = new TransportClient(ImmutableSettings.builder()
        .put("cluster.name", clusterName));
    for (String host : HOST_LIST_SPLITTER.split(hostList)) {
      client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
    }
    return client;
  }
}
