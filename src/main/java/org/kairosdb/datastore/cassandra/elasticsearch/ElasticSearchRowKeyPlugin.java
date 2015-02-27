package org.kairosdb.datastore.cassandra.elasticsearch;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.inject.Named;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.Order;
import org.kairosdb.datastore.cassandra.CassandraRowKeyPlugin;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.kairosdb.datastore.cassandra.CassandraDatastore.calculateRowTime;

/**
 * @author codyaray
 * @since 1/8/15
 */
public class ElasticSearchRowKeyPlugin implements CassandraRowKeyPlugin {

  private static final Logger log = LoggerFactory.getLogger(ElasticSearchRowKeyPlugin.class);

  private final Client client;
  private final String indexName;

  @Inject
  public ElasticSearchRowKeyPlugin(Client client, @Named(ElasticSearchModule.INDEX_NAME) String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  @Override
  public String getName() {
    return "elasticsearch";
  }


  private String oneline(Object o) {
    return Joiner.on(" ").join(Splitter.on("\n").split(o.toString()));
  }

  @Override
  public Iterator<DataPointsRowKey> getKeysForQueryIterator(DatastoreMetricQuery query) {
    BoolQueryBuilder queryBuilder = boolQuery()
        .must(termQuery("name", query.getName()))
        .must(rangeQuery("timestamp")
            .from(calculateRowTime(query.getStartTime()))
            .to(calculateRowTime(query.getEndTime()))); // by default, start and end are both inclusive
    for (List<String> tags : buildTagsQueries(query.getTags())) {
      queryBuilder.must(termsQuery("tags", tags));
    }

    SearchRequestBuilder request = client.prepareSearch(indexName)
        .setSearchType(SearchType.SCAN)
        .setScroll(new TimeValue(5000))
        .setQuery(queryBuilder)
        .addFields("name", "timestamp", "tags")
        .setSize(50);  // TODO: Inject a value later

    SearchResponse response = request
        .execute()
        .actionGet();

    List<String> joshAndCodysDirtyLogObject = Lists.newArrayList();
    joshAndCodysDirtyLogObject.add(String.format("SEARCH REQUEST: %s - SEARCH RESPONSE: %s", oneline(request), oneline(response)));

    SearchScrollRequestBuilder scrollRequestBuilder;


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

    ImmutableList<SearchHit> sortedHits = searchHitOrdering.immutableSortedCopy(hits);

    List<DataPointsRowKey> datapoints = FluentIterable.from(sortedHits)
        .transform(new Function<SearchHit, DataPointsRowKey>() {
          @Override
          public DataPointsRowKey apply(SearchHit hit) {
            Map<String, SearchHitField> fields = hit.getFields();
            return new DataPointsRowKey(
                fields.get("name").<String>getValue(),
                fields.get("timestamp").<Long>getValue(),
                hit.getType(),
                buildTagsField(fields.get("tags").getValues()));
          }
        }).toList();

    return datapoints.iterator();
  }

  private static List<List<String>> buildTagsQueries(SetMultimap<String, String> tags) {
    List<List<String>> allQueries = Lists.newArrayList(tags.size());
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
}
