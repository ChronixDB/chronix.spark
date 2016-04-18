/*
 * Copyright (C) 2016 QAware GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package de.qaware.chronix.spark.api.java;

import de.qaware.chronix.converter.KassiopeiaSimpleConverter;
import de.qaware.chronix.spark.api.java.timeseries.MetricTimeSeriesKey;
import de.qaware.chronix.spark.api.java.timeseries.MetricTimeSeriesOrdering;
import de.qaware.chronix.spark.api.java.util.SolrCloudUtil;
import de.qaware.chronix.spark.api.java.util.StreamingResultsIterator;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 *  A factory to create ChronixRDD instances.
 *
 *  Queries a SolrCloud to obtain time series data.
 *  Is bound to a JavaSparkContext.
 */
public class ChronixSparkContext implements Serializable {

    private transient JavaSparkContext jsc;

    public ChronixSparkContext(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    /**
     * Returns the associated Spark Context.
     * As the ChronixSparkContext does not handle the Spark Context
     * lifecycle the Spark Context can also be closed outside.
     *
     * @return the contained Spark Context
     */
    public JavaSparkContext getSparkContext(){
        return jsc;
    }

    /**
     * Creates a JavaRDD of SolrDocuments for a Solr query.
     *
     * @param query Solr query
     * @param zkHost ZooKeeper host URL
     * @param collection the Solr collection of chronix time series data
     * @return a JavaRDD of SolrDocuments as the Solr query result
     * @throws SolrServerException
     */
    public JavaRDD<SolrDocument> querySolr(
            final SolrQuery query,
            final String zkHost,
            final String collection) throws SolrServerException {
        // first get a list of replicas to query for this collection
        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.connect();
        List<String> shards = SolrCloudUtil.buildShardList(cloudSolrClient, collection);

        final SolrQuery enhancedQuery = query.getCopy();
        // we'll be directing queries to each shard, so we don't want distributed
        enhancedQuery.set("distrib", false);
        enhancedQuery.set("collection", collection);
        enhancedQuery.setStart(0);
        if (enhancedQuery.getRows() == null)
            enhancedQuery.setRows(SolrCloudUtil.CHRONIX_DEFAULT_PAGESIZE);
        enhancedQuery.setSort(SolrQuery.SortClause.asc("id"));

        // parallelize the requests to the shards
        return jsc.parallelize(shards, shards.size()).flatMap(
                new FlatMapFunction<String, SolrDocument>() {
                    public Iterable<SolrDocument> call(String shardUrl) throws Exception {
                        return new StreamingResultsIterator(
                                SolrCloudUtil.getSingleNodeSolrClient(shardUrl),
                                enhancedQuery, "*");
                    }
                }
        );
    }

    /**
     * Low-level chunked query.
     *
     * @param query
     * @param zkHost
     * @param collection the Solr collection of chronix time series data
     * @return
     * @throws SolrServerException
     */
    public ChronixRDD queryChronix(
            final SolrQuery query,
            final String zkHost,
            final String collection) throws SolrServerException {
        // first get a list of replicas to query for this collection
        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.connect();
        List<String> shards = SolrCloudUtil.buildShardList(cloudSolrClient, collection);

        // we'll be directing queries to each shard, so we don't want distributed
        final SolrQuery enhQuery = query.getCopy();
        enhQuery.set("distrib", false);

        // parallelize the requests to the shards
        JavaRDD<MetricTimeSeries> docs = jsc.parallelize(shards, shards.size()).flatMap(
                new FlatMapFunction<String, MetricTimeSeries>() {
                    public Iterable<MetricTimeSeries> call(String shardUrl) throws Exception {
                        ChronixSolrCloudStorage<MetricTimeSeries> chronixStorage
                                = new ChronixSolrCloudStorage<MetricTimeSeries>(SolrCloudUtil.CHRONIX_DEFAULT_PAGESIZE);
                        return chronixStorage.streamFromSingleNode(
                                new KassiopeiaSimpleConverter(),
                                SolrCloudUtil.getSingleNodeSolrClient(shardUrl),
                                enhQuery
                        )::iterator;
                    }
                }
        );
        return new ChronixRDD(docs);
    }

    /**
     * This method concats all timeseries chunks together to a single
     * MetrixTimeSeries Object. This method can be slow if the amount of
     * shuffeled data.
     *
     * @param query
     * @param zkHost
     * @param collection the Solr collection of chronix time series data
     * @return
     * @throws SolrServerException
     */
    public ChronixRDD query(
            final SolrQuery query,
            final String zkHost,
            final String collection) throws SolrServerException {

        ChronixRDD rootRdd = queryChronix(query, zkHost, collection);

        JavaPairRDD<MetricTimeSeriesKey, Iterable<MetricTimeSeries>> groupRdd
                = rootRdd.groupBy((MetricTimeSeries mts) -> {
            return new MetricTimeSeriesKey(mts);
        });

        JavaPairRDD<MetricTimeSeriesKey, MetricTimeSeries> joinedRdd;
        joinedRdd = groupRdd.mapValues((Iterable<MetricTimeSeries> mtsIt) -> {
            MetricTimeSeriesOrdering ordering = new MetricTimeSeriesOrdering();
            List<MetricTimeSeries> orderedChunks = ordering.immutableSortedCopy(mtsIt);
            MetricTimeSeries result = null;
            for (MetricTimeSeries mts : orderedChunks) {
                if (result == null) {
                    result = new MetricTimeSeries
                            .Builder(mts.getMetric())
                            .attributes(mts.getAttributesReference()).build();
                }
                result.addAll(mts.getTimestampsAsArray(), mts.getValuesAsArray());
            }
            return result;
        });

        JavaRDD<MetricTimeSeries> resultJavaRdd =
                joinedRdd.map((Tuple2<MetricTimeSeriesKey, MetricTimeSeries> mtTuple) -> {
                    return mtTuple._2;
                });

        return new ChronixRDD(resultJavaRdd);
    }
}