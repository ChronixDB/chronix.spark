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

import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.storage.solr.stream.SolrStreamingService;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.cloud.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @param <T> the type defining the returned type
 */
public class ChronixSolrCloudStorage<T> implements Serializable {

    private static final long serialVersionUID = 42L;

    private final int nrOfDocumentPerBatch;

    /**
     * The default pagesize for paginations within Solr.
     */
    public static final int CHRONIX_DEFAULT_PAGESIZE = 1000;


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

    public Stream<T> stream(TimeSeriesConverter<T> converter, String zkHost, SolrQuery query) {
        CloudSolrClient connection = new CloudSolrClient(zkHost);
        connection.setRequestWriter(new BinaryRequestWriter());
        LoggerFactory.getLogger(ChronixSolrCloudStorage.class).debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, connection, query);
        SolrStreamingService<T> solrStreamingService = new SolrStreamingService<>(converter, query, connection, nrOfDocumentPerBatch);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false);
    }

    /**
     * Fetches a stream of time series only from a single node.
     *
     * @param converter  the time series type converter
     * @param shardUrl the URL of the shard pointing to a single node
     * @param query      the solr query
     * @return a stream of time series
     */
    public Stream<T> streamFromSingleNode(TimeSeriesConverter<T> converter, String shardUrl, SolrQuery query) {
        HttpSolrClient solrClient = getSingleNodeSolrClient(shardUrl);
        solrClient.setRequestWriter(new BinaryRequestWriter());
        LoggerFactory.getLogger(ChronixSolrCloudStorage.class).debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, solrClient, query);
        SolrStreamingService<T> solrStreamingService = new SolrStreamingService<>(converter, query, solrClient, nrOfDocumentPerBatch);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false);
    }

    public boolean add(TimeSeriesConverter<T> converter, Collection<T> documents, CloudSolrClient connection) {
        throw new UnsupportedOperationException();
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
     * @param zkHost ZooKeeper URL
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
