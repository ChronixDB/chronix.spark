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
package de.qaware.chronix.storage.solr;

import de.qaware.chronix.Schema;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.storage.solr.stream.SolrStreamingService;
import de.qaware.chronix.storage.solr.stream.SolrTupleStreamingService;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.common.cloud.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ChronixSolrCloudStorage implements Serializable {

    /**
     * The default pagesize for paginations within Solr.
     */
    public static final int CHRONIX_DEFAULT_PAGESIZE = 1000;
    private static final long serialVersionUID = 42L;
    private final int nrOfDocumentPerBatch;
    private static final boolean EXPORT_HANDLER_STREAM = false;


    /**
     * Constructs a Chronix storage that is based on an apache solr cloud.
     *
     * @param nrOfDocumentPerBatch number of documents that are processed in one batch
     */
    public ChronixSolrCloudStorage(final int nrOfDocumentPerBatch) {
        this.nrOfDocumentPerBatch = nrOfDocumentPerBatch;
    }

    /**
     * Constructs a Chronix storage with default paging size (batch size)
     */
    public ChronixSolrCloudStorage() {
        this.nrOfDocumentPerBatch = CHRONIX_DEFAULT_PAGESIZE;
    }

    /**
     * Fetches a stream of time series only from a single node.
     *
     * @param shardUrl  the URL of the shard pointing to a single node
     * @param query     the solr query
     * @param converter the time series type converter
     * @return a stream of time series
     */
    public Stream<MetricTimeSeries> streamFromSingleNode(String zkHost,
                                                         String collection,
                                                         String shardUrl,
                                                         SolrQuery query,
                                                         TimeSeriesConverter<MetricTimeSeries> converter) throws IOException {
        if (EXPORT_HANDLER_STREAM) {
            return streamWithCloudSolrStream(zkHost, collection, shardUrl, query, converter);
        } else {
            return streamWithHttpSolrClient(shardUrl, query, converter);
        }
    }

    /**
     * Fetches a stream of time series only from a single node with CloudSolrStream.
     *
     * @param zkHost
     * @param collection
     * @param shardUrl
     * @param query
     * @param converter
     * @return
     * @throws IOException
     */
    private Stream<MetricTimeSeries> streamWithCloudSolrStream(String zkHost,
                                                               String collection,
                                                               String shardUrl,
                                                               SolrQuery query,
                                                               TimeSeriesConverter<MetricTimeSeries> converter) throws IOException {

        Map params = new HashMap();
        params.put("q", query.getQuery());
        params.put("sort", "id asc");
        params.put("shards", extractShardIdFromShardUrl(shardUrl));

        params.put("fl",
                Schema.DATA + ", " + Schema.ID + ", " + Schema.START + ", " + Schema.END +
                        ", metric, host, measurement, process, ag, group");
        params.put("qt", "/export");
        params.put("distrib", false);

        CloudSolrStream solrStream = new CloudSolrStream(zkHost, collection, params);
        solrStream.open();
        SolrTupleStreamingService tupStream = new SolrTupleStreamingService(solrStream, converter);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(tupStream, Spliterator.SIZED), false);
    }

    private String extractShardIdFromShardUrl(String shardUrl) {
        String[] shardSplits = shardUrl.split("/");
        if (shardSplits.length != 5) throw new RuntimeException(
                new MalformedURLException("Wrong shard URL: " + shardUrl));
        return shardSplits[4];
    }

    /**
     * Fetches a stream of time series only from a single node with HttpSolrClient.
     *
     * @param shardUrl
     * @param query
     * @param converter
     * @return
     */
    private Stream<MetricTimeSeries> streamWithHttpSolrClient(String shardUrl,
                                                              SolrQuery query,
                                                              TimeSeriesConverter<MetricTimeSeries> converter) {
        HttpSolrClient solrClient = getSingleNodeSolrClient(shardUrl);
        solrClient.setRequestWriter(new BinaryRequestWriter());
        query.set("distrib", false);
        LoggerFactory.getLogger(ChronixSolrCloudStorage.class).debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, solrClient, query);
        SolrStreamingService<MetricTimeSeries> solrStreamingService = new SolrStreamingService<>(converter, query, solrClient, nrOfDocumentPerBatch);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false);
    }

    /**
     * Returns a connection to a single Solr node by shard URL.
     *
     * @param shardUrl the url of the solr endpoint where the shard resides
     * @return a connection to a single Solr node within a Solr Cloud
     */
    private HttpSolrClient getSingleNodeSolrClient(String shardUrl) {
        return new HttpSolrClient(shardUrl);
    }

    /**
     * Returns the list of shards of the default collection.
     *
     * @param zkHost            ZooKeeper URL
     * @param chronixCollection Solr collection name for chronix time series data
     * @return the list of shards of the default collection
     */
    public List<String> getShardList(String zkHost, String chronixCollection) throws IOException {

        CloudSolrClient cloudSolrClient = new CloudSolrClient(zkHost);
        List<String> shards = new ArrayList<>();

        try {
            cloudSolrClient.connect();

            ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();

            ClusterState clusterState = zkStateReader.getClusterState();

            String[] collections;
            if (clusterState.hasCollection(chronixCollection)) {
                collections = new String[]{chronixCollection};
            } else {
                // might be a collection alias?
                Aliases aliases = zkStateReader.getAliases();
                String aliasedCollections = aliases.getCollectionAlias(chronixCollection);
                if (aliasedCollections == null)
                    throw new IllegalArgumentException("Collection " + chronixCollection + " not found!");
                collections = aliasedCollections.split(",");
            }

            Set<String> liveNodes = clusterState.getLiveNodes();
            Random random = new Random(5150);

            for (String coll : collections) {
                for (Slice slice : clusterState.getSlices(coll)) {
                    List<String> replicas = new ArrayList<>();
                    for (Replica r : slice.getReplicas()) {
                        if (r.getState().equals(Replica.State.ACTIVE)) {
                            ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
                            if (liveNodes.contains(replicaCoreProps.getNodeName()))
                                replicas.add(replicaCoreProps.getCoreUrl());
                        }
                    }
                    int numReplicas = replicas.size();
                    if (numReplicas == 0)
                        throw new IllegalStateException("Shard " + slice.getName() + " in collection " +
                                coll + " does not have any active replicas!");

                    String replicaUrl = (numReplicas == 1) ? replicas.get(0) : replicas.get(random.nextInt(replicas.size()));
                    shards.add(replicaUrl);
                }
            }
        } finally {
            cloudSolrClient.close();
        }

        return shards;
    }

}
