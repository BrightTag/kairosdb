package org.kairosdb.datastore.elasticstats;

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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
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

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
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

  private class Bucket {
    private String id;
    private long timestamp;
    private BucketMeta meta;

    public Bucket(String id, long timestamp, BucketMeta meta) {
      this.id = id;
      this.timestamp = timestamp;
      this.meta = meta;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Bucket other = (Bucket) obj;
      return Objects.equal(this.id, other.id) && Objects.equal(this.timestamp, other.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, timestamp);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("id", id)
          .add("timestamp", timestamp)
          .add("meta", meta)
          .toString();
    }
  }

  private class BucketMeta {
    private String type;
    private SortedMap<String, String> tags;

    public BucketMeta(String type, SortedMap<String, String> tags) {
      this.type = type;
      this.tags = tags;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      BucketMeta other = (BucketMeta) obj;
      return Objects.equal(this.type, other.type) && Objects.equal(this.tags, other.tags);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type, tags);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("type", type)
          .add("tags", tags)
          .toString();
    }
//    public boolean isPresentInTimeSlice() {
//      return timestamp != null && count != null;
//    }
  }

  private static final Ordering<Bucket> BY_TIMESTAMP = Ordering.natural().nullsLast()
      .onResultOf(new Function<Bucket, Long>() {
        @Override
        public @Nullable Long apply(@Nullable Bucket bucket) {
          return bucket == null ? null : bucket.timestamp;
        }
      })
      .compound(Ordering.natural().nullsLast()
          .onResultOf(new Function<Bucket, String>() {
              @Override
              public @Nullable String apply(@Nullable Bucket bucket) {
                return bucket == null ? null : bucket.id;
              }
            }
          )
      );

  @Override
  public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException {
    List<SearchHit> hits = doQuery(query);
    try {
      Map<String, BucketMeta> byId = Maps.newHashMapWithExpectedSize(hits.size());
      for (SearchHit hit : hits) {
        byId.put(hit.getId(), new BucketMeta(hit.getType(), buildTagsField(hit.getFields().get("tags").getValues())));
      }
//      log.info("ID IN {}, {} >= timestamp < {}", byId.keySet(), query.getStartTime(), query.getEndTime());
      DBCursor cursor = mongoClient.find(new BasicDBObject(
          "id", new BasicDBObject(
          "$in", byId.keySet())).append(
          "timestamp", new BasicDBObject(
              "$gte", query.getStartTime()).append(
              "$lt", query.getEndTime())));

      Map<Bucket, Integer> counts = Maps.newHashMap();
//      Map<Long, Integer> mongoTimestampCounts = Maps.newHashMap();
      try {
        while (cursor.hasNext()) {
          BasicDBObject res = (BasicDBObject) cursor.next();
          String id = res.getString("id");
          Bucket bucket = new Bucket(id, res.getLong("timestamp"), byId.get(id));
          if (counts.containsKey(bucket)) {
            log.error("Counts already contains bucket ({},{})", bucket.id, bucket.timestamp);
          }
          counts.put(bucket, res.getInt("count"));
//          if (entry.timestamp != null || entry.count != null) {
//            log.error("PRE EXISTING ENTRY STUFF : {}=>{} and {}=>{}", entry.timestamp, res.getLong("timestamp"), entry.count, res.getLong("count"));
//          }
//          entry.timestamp = res.getLong("timestamp");
//          entry.count = res.getLong("count");
//          log.info("Timestamp: {}", entry.timestamp);
//          mongoTimestampCounts.put(entry.timestamp, Objects.firstNonNull(mongoTimestampCounts.get(entry.timestamp), 0) + 1);
        }
      } finally {
        cursor.close();
      }

      // CALLBACK TIMESTAMP IS ALWAYS THE SAME!! BUGGGGGG
      Map<Long, Integer> callbackTimestampCounts = Maps.newHashMap();
      SortedMap<Bucket, Integer> sortedCounts = ImmutableSortedMap.<Bucket, Integer>orderedBy(BY_TIMESTAMP).putAll(counts).build();
      for (Map.Entry<Bucket, Integer> entry : sortedCounts.entrySet()) {
        Bucket bucket = entry.getKey();
        callbackTimestampCounts.put(bucket.timestamp, Objects.firstNonNull(callbackTimestampCounts.get(bucket.timestamp), 0) + 1);
//        if (bucketMeta.isPresentInTimeSlice()) {
          // Most naive implementation possible
//          log.info("CALLBACK Timestamp: {}", bucketCount.timestamp);
          queryCallback.startDataPointSet(bucket.meta.type, bucket.meta.tags);
          queryCallback.addDataPoint(new LongDataPoint(bucket.timestamp, entry.getValue()));
          queryCallback.endDataPoints();
//        }
      }

//      MapDifference<Long, Integer> diff = Maps.difference(mongoTimestampCounts, callbackTimestampCounts);
//      log.info("Difference: {}", diff);

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

//    log.info(Joiner.on(" | ").join(joshAndCodysDirtyLogObject));
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
