package org.kairosdb.datastore.elasticstats;

import java.net.UnknownHostException;
import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Named;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

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
public class ElasticstatsModule extends AbstractModule {

  private static final Logger logger = LoggerFactory.getLogger(ElasticstatsModule.class);

  static final String ES_HOST_LIST_PROPERTY = "kairosdb.datastore.elasticsearch.host_list";
  static final String ES_CLUSTER_NAME_PROPERTY = "kairosdb.datastore.elasticsearch.cluster_name";
  static final String ES_MAX_RECORD_COUNT = "kairosdb.datastore.elasticsearch.max_record_count";

  static final String MONGO_HOST_LIST_PROPERTY = "kairosdb.datastore.mongo.host_list";
  static final String MONGO_DATABASE_PROPERTY = "kairosdb.datastore.mongo.database";
  static final String MONGO_COLLECTION_PROPERTY = "kairosdb.datastore.mongo.collection";

  @Override
  protected void configure() {
    bind(Datastore.class).to(ElasticstatsDatastore.class).in(Singleton.class);
  }

  private static final Splitter HOST_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  @Provides
  Client provideElasticsearchClient(@Named(ES_HOST_LIST_PROPERTY) String hostList,
      @Named(ES_CLUSTER_NAME_PROPERTY) String clusterName) {
    logger.info("Using elasticsearch hosts '{}' and cluster name '{}'", hostList, clusterName);
    TransportClient client = new TransportClient(ImmutableSettings.builder()
        .put("cluster.name", clusterName));
    for (String host : HOST_LIST_SPLITTER.split(hostList)) {
      client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
    }
    return client;
  }

  @Provides
  DBCollection provideMongoCollection(
      @Named(MONGO_HOST_LIST_PROPERTY) String hostList,
      @Named(MONGO_DATABASE_PROPERTY) String database,
      @Named(MONGO_COLLECTION_PROPERTY) String collection) {
    List<ServerAddress> hosts = FluentIterable.from(HOST_LIST_SPLITTER.split(hostList))
        .transform(new Function<String, ServerAddress>() {
          @Override
          public @Nullable ServerAddress apply(String hostname) {
            try {
              return new ServerAddress(hostname);
            } catch (UnknownHostException e) {
              logger.warn("Unknown host: %s - %s", hostname, e);
              return null;
            }
          }
        })
        .toList();
    MongoClient client;
    if (!hosts.isEmpty()) {
      client = new MongoClient(hosts);
    } else {
      try {
        client = new MongoClient(hostList);
      } catch (UnknownHostException e) {
        logger.warn("Unknown host: %s - %s", hostList, e);
        throw Throwables.propagate(e);
      }
    }
    logger.info("Database: {}, Collection: {}", database, collection);
    return client.getDB(database).getCollection(collection);
  }

  @Provides @Named(ES_MAX_RECORD_COUNT)
  int provideLimitSize() {
    return Integer.getInteger(ES_MAX_RECORD_COUNT, 50);
  }
}
