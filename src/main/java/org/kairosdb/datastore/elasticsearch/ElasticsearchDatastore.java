package org.kairosdb.datastore.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.annotation.Nullable;
import javax.inject.Named;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.datastore.TagSetImpl;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * @author codyaray
 * @since 2/10/15
 */
public class ElasticsearchDatastore implements Datastore {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchDatastore.class);

  private static final String DATAPOINTS_INDEX = "kairosdb";

  private final Client client;
  private final int limitSize;

  @Inject
  public ElasticsearchDatastore(Client client,
      @Named(ElasticsearchModule.MAX_RECORD_COUNT) int limitSize) {
    this.client = client;
    this.limitSize = limitSize;
  }

  @Override
  public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException {
    List<SearchHit> hits = doQuery(query);
    try {
      for (SearchHit hit : hits) {
        Map<String, SearchHitField> fields = hit.getFields();
        // Most naive implementation possible
        queryCallback.startDataPointSet(hit.getType(), buildTagsField(fields.get("tags").getValues()));
        queryCallback.addDataPoint(new LongDataPoint(fields.get("timestamp").<Long>getValue(), fields.get("value").<Integer>getValue()));
        queryCallback.endDataPoints();
      }
    } catch (IOException e) {
      log.error("Problem querying Elasticsearch", e);
    }
  }

  @Override
  public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint) throws DatastoreException {
    try {
      XContentBuilder source = jsonBuilder()
          .startObject()
          .field("name", metricName)
          .field("timestamp", dataPoint.getTimestamp())
          .field("value", dataPoint.getLongValue())
          .array("tags", buildTagsArray(tags))
          .endObject();

      IndexRequestBuilder request = client.prepareIndex(DATAPOINTS_INDEX, dataPoint.getDataStoreDataType(), id(metricName, dataPoint.getTimestamp(), tags))
          .setSource(source);
      IndexResponse response = request
          .execute()
          .actionGet();
      log.info("INDEX REQUEST {}\nINDEX RESPONSE: {}", source.string(), indexResponseToString(response));
    } catch (IOException e) {
      log.error(String.format("Failed to store %s at %s with tags %s", metricName, dataPoint.getTimestamp(), tags), e);
    }
  }

  @Override
  public void deleteDataPoints(DatastoreMetricQuery query) throws DatastoreException {
    QueryBuilder queryBuilder = buildQuery(query);
    DeleteByQueryRequestBuilder request = client.prepareDeleteByQuery(DATAPOINTS_INDEX)
        .setQuery(queryBuilder);
    DeleteByQueryResponse response = request
        .execute()
        .actionGet();
    log.debug("DELETE REQUEST {}\nDELETE RESPONSE {}", request, response);
  }

  @Override
  public Iterable<String> getMetricNames() throws DatastoreException {
    SearchResponse response = client.prepareSearch(DATAPOINTS_INDEX)
        .addAggregation(AggregationBuilders
            .terms("metric_name")
            .field("name")
            .order(Terms.Order.term(true))
            .size(0)
            .shardSize(0))
        .execute()
        .actionGet();
    Terms metrics = response.getAggregations().get("metric_name");
    return FluentIterable.from(metrics.getBuckets())
        .transform(new Function<Terms.Bucket, String>() {
          @Override
          public String apply(Terms.Bucket termBucket) {
            return termBucket.getKey();
          }
        })
        .toList();
  }

  @Override
  public Iterable<String> getTagNames() throws DatastoreException {
    return queryTagsList(NameOrValue.NAME);
  }

  @Override
  public Iterable<String> getTagValues() throws DatastoreException {
    return queryTagsList(NameOrValue.VALUE);
  }

  @Override
  public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException {
    List<SearchHit> hits = doQuery(query);
    TagSetImpl tagSet = new TagSetImpl();
    for (SearchHit hit : hits) {
      Map<String, SearchHitField> fields = hit.getFields();
      SortedMap<String, String> tags = buildTagsField(fields.get("tags").getValues());
      for (Map.Entry<String, String> tag : tags.entrySet()) {
        tagSet.addTag(tag.getKey(), tag.getValue());
      }
    }
    return tagSet;
  }

  @Override
  public void close() throws InterruptedException, DatastoreException {
    client.close();
  }

  private String oneline(Object o) {
    return Joiner.on(" ").join(Splitter.on("\n").split(o.toString()));
  }

	private List<SearchHit> doQuery(DatastoreMetricQuery query) {
		QueryBuilder queryBuilder = buildQuery(query);
		SearchRequestBuilder request = client.prepareSearch(DATAPOINTS_INDEX)
        .setSearchType(SearchType.SCAN)
        .setScroll(new TimeValue(5000))
				.setQuery(queryBuilder)
				.addFields("name", "timestamp", "tags", "value")
				.setSize(limitSize);

		SearchResponse response = request
				.execute()
				.actionGet();

    SearchScrollRequestBuilder scrollRequestBuilder;

    List<String> joshAndCodysDirtyLogObject = Lists.newArrayList();

    joshAndCodysDirtyLogObject.add(String.format("SEARCH REQUEST: %s - SEARCH RESPONSE: %s", oneline(request), oneline(response)));

    List<SearchHit> hits = Lists.newArrayList();
    do {

      hits.addAll(ImmutableList.copyOf(response.getHits().getHits()));
      scrollRequestBuilder = client.prepareSearchScroll(response.getScrollId())
          .setScroll(new TimeValue(5000));

      response = scrollRequestBuilder.execute()
          .actionGet();

      joshAndCodysDirtyLogObject.add(String.format("SCROLL REQUEST: %s - SCROLL RESPONSE: %s", oneline(scrollRequestBuilder), oneline(response)));
    } while (response.getHits().getHits().length > 0);

    Ordering<SearchHit> searchHitOrdering = new Ordering<SearchHit>() {
      public int compare(SearchHit left, SearchHit right) {
        return Longs.compare(left.getFields().get("timestamp").<Long>getValue(), right.getFields().get("timestamp").<Long>getValue());
      }
    };


    log.info(Joiner.on(" | ").join(joshAndCodysDirtyLogObject));

    return searchHitOrdering.immutableSortedCopy(hits);
	}

  private static Splitter TAG_SPLITTER = Splitter.on('=').trimResults();

  private List<String> queryTagsList(final NameOrValue nameOrValue) {
    SearchResponse response = client.prepareSearch(DATAPOINTS_INDEX)
        .addAggregation(AggregationBuilders
            .terms("metric_tags")
            .field("tags")
            .order(Terms.Order.term(true))
            .size(0)
            .shardSize(0))
        .execute()
        .actionGet();
    Terms metrics = response.getAggregations().get("metric_tags");
    return FluentIterable.from(metrics.getBuckets())
        .transform(new Function<Terms.Bucket, String>() {
          @Override
          public String apply(Terms.Bucket termBucket) {
            return termBucket.getKey();
          }
        })
        .transform(new Function<String, String>() {
          @Override
          public String apply(String tag) {
            List<String> keyValue = Lists.newArrayList(TAG_SPLITTER.split(tag));
            return keyValue.get(nameOrValue.getPosition());
          }
        })
        .toList();
  }

  private static enum NameOrValue {
    NAME(0), VALUE(1);

    private final int position;

    private NameOrValue(int position) {
      this.position = position;
    }

    int getPosition() {
      return position;
    }
  }

  private BoolQueryBuilder buildQuery(DatastoreMetricQuery query) {
    BoolQueryBuilder queryBuilder = boolQuery()
        .must(termQuery("name", query.getName()))
        .must(rangeQuery("timestamp")
            .from(query.getStartTime())
            .to(query.getEndTime())); // by default, start and end are both inclusive
    for (List<String> tags : buildTagsQueries(query.getTags())) {
      queryBuilder.must(termsQuery("tags", tags));
    }
    return queryBuilder;
  }

  private static List<List<String>> buildTagsQueries(SetMultimap<String, String> tags) {
    List<List<String>> allQueries = Lists.newArrayListWithExpectedSize(tags.size());
    for (Map.Entry<String, Collection<String>> tag : tags.asMap().entrySet()) {
      ImmutableList.Builder<String> tagQueriesBuilder = ImmutableList.builder();
      for (String value : tag.getValue()) {
        tagQueriesBuilder.add(String.format("%s=%s", tag.getKey(), value));
      }
      allQueries.add(tagQueriesBuilder.build());
    }
    return allQueries;
  }

  private static SortedMap<String, String> buildTagsField(List<Object> tags) {
    SortedMap<String, String> allTags = Maps.newTreeMap();
    for (Object tag : tags) {
      String[] keyValue = tag.toString().split("=");
      if (keyValue.length != 2) {
        throw new RuntimeException("Invalid tag string: " + tag);
      }
      allTags.put(keyValue[0], keyValue[1]);
    }
    return allTags;
  }

  private static String[] buildTagsArray(SortedMap<String, String> tags) {
    return FluentIterable.from(tags.entrySet())
        .transform(new Function<Map.Entry<String, String>, String>() {
          @Override
          public @Nullable String apply(@Nullable Map.Entry<String, String> tag) {
            return tag == null ? null : String.format("%s=%s", tag.getKey(), tag.getValue());
          }
        })
        .toArray(String.class);
  }

  private static Joiner ROW_KEY_ID_JOINER = Joiner.on('|');

  private static String id(String metricName, long timestamp, ImmutableSortedMap<String, String> tags) {
    return ROW_KEY_ID_JOINER.join(metricName, timestamp, tags);
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
