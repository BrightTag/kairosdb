package org.kairosdb.datastore.cassandra.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

import javax.annotation.Nullable;
import javax.inject.Named;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.kairosdb.datastore.cassandra.RowKeyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * @author codyaray
 * @since 1/8/15
 */
public class ElasticSearchRowKeyListener implements RowKeyListener {

  private static final Logger log = LoggerFactory.getLogger(ElasticSearchRowKeyListener.class);

  private final Client client;
  private final String indexName;

  @Inject
  public ElasticSearchRowKeyListener(Client client, @Named(ElasticSearchModule.INDEX_NAME) String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  @Override
  public void addRowKey(String metricName, DataPointsRowKey rowKey, int rowKeyTtl) {
    try {
      XContentBuilder source = jsonBuilder()
          .startObject()
            .field("name", metricName)
            .field("timestamp", rowKey.getTimestamp())
            .array("tags", buildTagsArray(rowKey.getTags()));
      if (rowKeyTtl > 0) {
        source = source.field("_ttl", rowKeyTtl);
      }
      source = source.endObject();

      IndexRequestBuilder request = client.prepareIndex(indexName, rowKey.getDataType(), id(rowKey))
          .setSource(source);
      IndexResponse response = request
          .execute()
          .actionGet();
      log.info("INDEX REQUEST {}\nINDEX RESPONSE: {}", source.string(), indexResponseToString(response));
    } catch (IOException e) {
      log.error(String.format("Failed to index %s with ttl %d", rowKey, rowKeyTtl), e);
    }
  }

  private static Joiner ROW_KEY_ID_JOINER = Joiner.on('|');

  private static String id(DataPointsRowKey rowKey) {
    return ROW_KEY_ID_JOINER.join(rowKey.getMetricName(), rowKey.getTimestamp(), rowKey.getTags());
  }

  private static String[] buildTagsArray(SortedMap<String, String> tags) {
    return FluentIterable.from(tags.entrySet())
        .transform(new Function<Map.Entry<String, String>, String>() {
          @Override
          public @Nullable
          String apply(@Nullable Map.Entry<String, String> tag) {
            return tag == null ? null : String.format("%s=%s", tag.getKey(), tag.getValue());
          }
        })
        .toArray(String.class);
  }

  private static String indexResponseToString(IndexResponse response) {
    return Objects.toStringHelper(response.getClass())
        .add("index", response.getIndex())
        .add("type", response.getType())
        .add("id", response.getId())
        .add("version", response.getVersion())
        .toString();
  }
}
