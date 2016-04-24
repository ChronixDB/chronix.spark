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
import de.qaware.chronix.spark.api.java.timeseries.MetricDimensions;
import de.qaware.chronix.spark.api.java.timeseries.MetricObservation;
import de.qaware.chronix.spark.api.java.timeseries.MetricTimeSeriesKey;
import de.qaware.chronix.spark.api.java.timeseries.MetricTimeSeriesOrdering;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A factory to create ChronixRDD instances.
 * <p>
 * Queries a SolrCloud to obtain time series data.
 * Is bound to a JavaSparkContext.
 */
public class ChronixSparkContext implements Serializable {

    private final transient JavaSparkContext jsc;

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
    public JavaSparkContext getSparkContext() {
        return jsc;
    }


    /**
     * Low-level chunked query.
     *
     * @param query Solr query
     * @param zkHost Zookeeper host
     * @param collection     the Solr collection of chronix time series data
     * @param chronixStorage a ChronixSolrCloudStorage instance
     * @return ChronixRDD of time series (chunks)
     * @throws SolrServerException
     */
    public ChronixRDD queryChronixChunks(
            final SolrQuery query,
            final String zkHost,
            final String collection,
            final ChronixSolrCloudStorage<MetricTimeSeries> chronixStorage) throws SolrServerException, IOException {

        // first get a list of replicas to query for this collection
        List<String> shards = chronixStorage.getShardList(zkHost, collection);

        // we'll be directing queries to each shard, so we don't want distributed
        final SolrQuery enhQuery = query.getCopy();
        enhQuery.set("distrib", false);

        // parallelize the requests to the shards
        JavaRDD<MetricTimeSeries> docs = jsc.parallelize(shards, shards.size()).flatMap(
                (FlatMapFunction<String, MetricTimeSeries>) shardUrl ->
                        chronixStorage.streamFromSingleNode(
                                new KassiopeiaSimpleConverter(),
                                shardUrl,
                                enhQuery)::iterator
        );
        return new ChronixRDD(docs);
    }

    /**
     * This method concats all timeseries chunks together to a single
     * MetrixTimeSeries Object.
     *
     * @param query Solr query
     * @param zkHost ZooKeeper host
     * @param collection     the Solr collection of chronix time series data
     * @param chronixStorage a ChronixSolrCloudStorage instance
     * @return ChronixRDD of joined time series (without chunks)
     * @throws SolrServerException
     */
    public ChronixRDD query(
            final SolrQuery query,
            final String zkHost,
            final String collection,
            final ChronixSolrCloudStorage<MetricTimeSeries> chronixStorage) throws SolrServerException, IOException {

        ChronixRDD rootRdd = queryChronixChunks(query, zkHost, collection, chronixStorage);

        JavaPairRDD<MetricTimeSeriesKey, Iterable<MetricTimeSeries>> groupRdd
                = rootRdd.groupBy(MetricTimeSeriesKey::new);

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
                joinedRdd.map((Tuple2<MetricTimeSeriesKey, MetricTimeSeries> mtTuple) -> mtTuple._2);

        return new ChronixRDD(resultJavaRdd);
    }

    /**
     * Tunes the give Spark Configuration with additional parameters
     * to optimize memory consumption and execution performance for
     * Chronix.
     *
     * @param conf Spark Configuration. Will be modified by the method.
     */
    public static void tuneSparkConf(SparkConf conf) {
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.rdd.compress", "true"); //activates compression of materialized RDD partitions
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");
        conf.registerKryoClasses(new Class[]{
                MetricTimeSeries.class,
                MetricObservation.class,
                MetricDimensions.class,
                MetricTimeSeriesKey.class,
                MetricTimeSeriesOrdering.class,
                ChronixRDD.class,
                ChronixSparkContext.class,
                ChronixSolrCloudStorage.class});
    }
}