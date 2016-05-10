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

import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.storage.solr.converter.MetricTimeSeriesConverter;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

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

    private static final long serialVersionUID = 42L;

    private final transient JavaSparkContext jsc;

    /**
     * Default constructor for a ChronixSparkContext.
     *
     * @param jsc an initialized JavaSparkContext
     */
    public ChronixSparkContext(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    /**
     * Additional constructor for a ChronixSparkContext to be
     * constructed from a plain SparkContext. for convenience if
     * used in Apache Zeppelin.
     *
     * @param sc an initialized SparkContext
     */
    public ChronixSparkContext(SparkContext sc) {
        this.jsc = JavaSparkContext.fromSparkContext(sc);
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
            final ChronixSolrCloudStorage chronixStorage) throws SolrServerException, IOException {

        // first get a list of replicas to query for this collection
        List<String> shards = chronixStorage.getShardList(zkHost, collection);

        // parallelize the requests to the shards
        JavaRDD<MetricTimeSeries> docs = jsc.parallelize(shards, shards.size()).flatMap(
                (FlatMapFunction<String, MetricTimeSeries>) shardUrl -> chronixStorage.streamFromSingleNode(
                        zkHost, collection, shardUrl, query, new MetricTimeSeriesConverter())::iterator);
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
            final ChronixSolrCloudStorage chronixStorage) throws SolrServerException, IOException {
        return queryChronixChunks(query, zkHost, collection, chronixStorage).joinChunks();
    }

    /**
     * Tunes the give Spark Configuration with additional parameters
     * to optimize memory consumption and execution performance for
     * Chronix.
     * <ul>
     *     <li>Disables the Spark web UI</li>
     *     <li>Enables the RDD compression to save memory space</li>
     *     <li>Sets the compression codec to LZF</li>
     * </ul>
     *
     * @param conf Spark Configuration. Will be modified by the method.
     */
    public static void tuneSparkConf(SparkConf conf) {
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.rdd.compress", "true"); //activates compression of materialized RDD partitions
        conf.set("spark.io.compression.codec", "org.apache.spark.io.LZFCompressionCodec");
    }
}