package org.kairosdb.datastore.elasticstats;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import javax.annotation.Nullable;
import javax.inject.Named;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

/**
 * @author codyaray
 * @since 3/30/15
 */
public class ElasticstatsDatastore implements Datastore {

  private static final Logger log = LoggerFactory.getLogger(ElasticstatsDatastore.class);

  private static final String DATAPOINTS_INDEX = "kairosdb";

  private final Client client;
  private final DBCollection mongoClient;
  private final int esLimitSize;

  @Inject
  public ElasticstatsDatastore(Client esClient, DBCollection mongoClient,
      @Named(ElasticstatsModule.ES_MAX_RECORD_COUNT) int esLimitSize) {
    this.client = esClient;
    this.mongoClient = mongoClient;
    this.esLimitSize = esLimitSize;
  }

  private class BucketCount {
    private final long timestamp;
    private final long count;

    public BucketCount(long timestamp, long count) {
      this.timestamp = timestamp;
      this.count = count;
    }
  }

  @Override
  public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException {
    List<SearchHit> hits = doQuery(query);
    try {
      Map<String, SearchHit> hitsById = Maps.newHashMapWithExpectedSize(hits.size());
      for (SearchHit hit : hits) {
        hitsById.put(hit.getId(), hit);
      }
      DBCursor cursor = mongoClient.find(new BasicDBObject(
          "id", new BasicDBObject(
              "$in", hitsById.keySet())).append(
          "timestamp", new BasicDBObject(
              "$gte", query.getStartTime()).append(
              "$lt", query.getEndTime())));

      final Map<String, BucketCount> bucketCountsById = Maps.newHashMapWithExpectedSize(hits.size());
      try {
        while (cursor.hasNext()) {
          BasicDBObject res = (BasicDBObject) cursor.next();
          bucketCountsById.put(res.getString("id"), new BucketCount(res.getLong("timestamp"), res.getInt("count")));
        }
      } finally {
        cursor.close();
      }

      Set<String> noCount = Sets.difference(hitsById.keySet(), bucketCountsById.keySet());
      Set<String> noHit = Sets.difference(bucketCountsById.keySet(), hitsById.keySet());
      if (!noCount.isEmpty()) {
        log.warn("Missing data for index: {}", noCount);
      }
      if (!noHit.isEmpty()) {
        // This shouldn't be possible, since we only query for what the index returns
        log.warn("Missing index for data: {}", noHit);
      }

      Ordering<String> byTimestamp = Ordering.natural().nullsLast().onResultOf(new Function<String, Long>() {
        @Override
        public @Nullable Long apply(@Nullable String id) {
          return id == null ? null : bucketCountsById.get(id).timestamp;
        }
      });

      for (Map.Entry<String, BucketCount> entry : ImmutableSortedMap.copyOf(bucketCountsById, byTimestamp).entrySet()) {
        BucketCount bucketCount = entry.getValue();
        SearchHit hit = hitsById.get(entry.getKey());

        // Most naive implementation possible
        Map<String, SearchHitField> fields = hit.getFields();
        queryCallback.startDataPointSet(hit.getType(), buildTagsField(fields.get("tags").getValues()));
        queryCallback.addDataPoint(new LongDataPoint(bucketCount.timestamp, bucketCount.count));
        queryCallback.endDataPoints();
      }
    } catch (IOException e) {
      log.error("Problem querying Elasticsearch", e);
    }
  }

  @Override
  public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint) throws DatastoreException {
//    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteDataPoints(DatastoreMetricQuery query) throws DatastoreException {
//    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<String> getMetricNames() throws DatastoreException {
    SearchResponse response = client.prepareSearch(DATAPOINTS_INDEX)
        .addAggregation(AggregationBuilders
            .terms("metric_name")
            .field("name")
            .order(Terms.Order.term(true)) // ascending
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
        .addFields("_id", "name", "tags")
        .setSize(esLimitSize);

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

    log.info(Joiner.on(" | ").join(joshAndCodysDirtyLogObject));
    return hits;
  }

  private static Splitter TAG_SPLITTER = Splitter.on('=').trimResults();

  private List<String> queryTagsList(final NameOrValue nameOrValue) {
    SearchResponse response = client.prepareSearch(DATAPOINTS_INDEX)
        .addAggregation(AggregationBuilders
            .terms("metric_tags")
            .field("tags")
            .order(Terms.Order.term(true)) // ascending
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
        .must(termQuery("name", query.getName()));
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
          public @Nullable
          String apply(@Nullable Map.Entry<String, String> tag) {
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
