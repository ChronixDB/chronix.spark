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
package de.qaware.chronix.spark.api.java.spark.api.java;

import de.qaware.chronix.spark.api.java.spark.api.java.util.SolrCloudUtil;
import de.qaware.chronix.spark.api.java.spark.api.java.util.StreamingResultsIterator;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.storage.solr.converter.MetricTimeSeriesConverter;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.List;

/**
 *  A factory to create ChronixRDD instances.
 *
 *  Queries a SolrCloud to obtain time series data.
 *  Furthermore the factory can provide test time series data.
 */
public class ChronixSparkContext implements Serializable {

    private transient JavaSparkContext jsc;

    public ChronixSparkContext(JavaSparkContext jsc) {
        this.jsc = jsc;
    }

    public JavaSparkContext getSparkContext(){
        return jsc;
    }

    /**
     *
     * @param origQuery
     * @param zkHost
     * @return
     * @throws SolrServerException
     */
    public JavaRDD<SolrDocument> querySolr(
            final SolrQuery origQuery,
            final String zkHost) throws SolrServerException {
        // first get a list of replicas to query for this collection
        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.connect();
        List<String> shards = SolrCloudUtil.buildShardList(cloudSolrClient);

        final SolrQuery query = origQuery.getCopy();

        // we'll be directing queries to each shard, so we don't want distributed
        query.set("distrib", false);
        query.set("collection", SolrCloudUtil.CHRONIX_COLLECTION);
        query.setStart(0);
        if (query.getRows() == null)
            query.setRows(SolrCloudUtil.CHRONIX_DEFAULT_PAGESIZE); // default page size
        query.setSort(SolrQuery.SortClause.asc("id")); // JW: check this

        // parallelize the requests to the shards
        JavaRDD<SolrDocument> docs = jsc.parallelize(shards, shards.size()).flatMap(
                new FlatMapFunction<String, SolrDocument>() {
                    public Iterable<SolrDocument> call(String shardUrl) throws Exception {
                        return new StreamingResultsIterator(
                                SolrCloudUtil.getSingleNodeSolrClient(shardUrl),
                                query, "*");
                    }
                }
        );
        return docs;
    }

    /**
     *
     * @param query
     * @param zkHost
     * @return
     * @throws SolrServerException
     */
    public ChronixRDD queryChronix(
            final SolrQuery query,
            final String zkHost) throws SolrServerException {
        // first get a list of replicas to query for this collection
        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        cloudSolrClient.connect();
        List<String> shards = SolrCloudUtil.buildShardList(cloudSolrClient);

        // parallelize the requests to the shards
        // TODO: provide data locality
        JavaRDD<MetricTimeSeries> docs = jsc.parallelize(shards, shards.size()).flatMap(
                new FlatMapFunction<String, MetricTimeSeries>() {
                    public Iterable<MetricTimeSeries> call(String shardUrl) throws Exception {
                        ChronixSolrCloudStorage<MetricTimeSeries> chronixStorage
                                = new ChronixSolrCloudStorage<MetricTimeSeries>(SolrCloudUtil.CHRONIX_DEFAULT_PAGESIZE);
                        return chronixStorage.streamFromSingleNode(
                                new MetricTimeSeriesConverter(),
                                SolrCloudUtil.getSingleNodeSolrClient(shardUrl),
                                query
                        )::iterator;
                    }
                }
        );
        return new ChronixRDD(docs);
    }

    /**
     *
     * @return a bunch of time series test data
     */
    public ChronixRDD getTimeSeriesTestData(){
        return null;
    }

}
