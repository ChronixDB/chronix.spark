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
package de.qaware.chronix.spark.api.java.util;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Collection of some static utility methods to ease usage of
 * Solr Cloud within Chronix Spark.
 */
public class SolrCloudUtil {

    /**
     * The default Solr collection for time series data.
     * Defaul name is Chronix-wide "ekgdata" due to historical reasons.
     */
    public static final String CHRONIX_COLLECTION = "ekgdata";

    /**
     * The default pagesize for paginations within Solr.
     */
    public static final int CHRONIX_DEFAULT_PAGESIZE = 1000;

    /**
     * Returns a connection to a single Solr node by shard URL.
     *
     * @param shardUrl
     * @return a connection to a single Solr node within a Solr Cloud
     */
    public static HttpSolrClient getSingleNodeSolrClient(String shardUrl){
        HttpSolrClient hsc = new HttpSolrClient(shardUrl);
        return hsc;
    }

    /**
     * Returns the list of shards of the default collection.
     *
     * @param cloudSolrServer
     * @return the list of shards of the default collection
     */
    public static List<String> buildShardList(CloudSolrClient cloudSolrServer) {
        ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();

        ClusterState clusterState = zkStateReader.getClusterState();

        String[] collections = null;
        if (clusterState.hasCollection(CHRONIX_COLLECTION)) {
            collections = new String[]{CHRONIX_COLLECTION};
        } else {
            // might be a collection alias?
            Aliases aliases = zkStateReader.getAliases();
            String aliasedCollections = aliases.getCollectionAlias(CHRONIX_COLLECTION);
            if (aliasedCollections == null)
                throw new IllegalArgumentException("Collection " + CHRONIX_COLLECTION + " not found!");
            collections = aliasedCollections.split(",");
        }

        Set<String> liveNodes = clusterState.getLiveNodes();
        Random random = new Random(5150);

        List<String> shards = new ArrayList<String>();
        for (String coll : collections) {
            for (Slice slice : clusterState.getSlices(coll)) {
                List<String> replicas = new ArrayList<String>();
                for (Replica r : slice.getReplicas()) {
                    if (r.getState().equals(Replica.State.ACTIVE)) {
                        ZkCoreNodeProps replicaCoreProps = new ZkCoreNodeProps(r);
                        if (liveNodes.contains(replicaCoreProps.getNodeName()))
                            replicas.add(replicaCoreProps.getCoreUrl());
                    }
                }
                int numReplicas = replicas.size();
                if (numReplicas == 0)
                    throw new IllegalStateException("Shard " + slice.getName() + " in collection "+
                            coll+" does not have any active replicas!");

                String replicaUrl = (numReplicas == 1) ? replicas.get(0) : replicas.get(random.nextInt(replicas.size()));
                shards.add(replicaUrl);
            }
        }
        return shards;
    }

    /**
     * Performs a Solr query.
     *
     * @param solrServer
     * @param solrQuery
     * @param startIndex
     * @param cursorMark
     * @return
     * @throws SolrServerException
     */
    public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark) throws SolrServerException {
        return querySolr(solrServer, solrQuery, startIndex, cursorMark, null);
    }

    /**
     * Performs a Solr query.
     *
     * @param solrServer
     * @param solrQuery
     * @param startIndex
     * @param cursorMark
     * @param callback
     * @return
     * @throws SolrServerException
     */
    public static QueryResponse querySolr(SolrClient solrServer, SolrQuery solrQuery, int startIndex, String cursorMark, StreamingResponseCallback callback) throws SolrServerException {
        QueryResponse resp = null;
        try {
            if (cursorMark != null) {
                solrQuery.setStart(0);
                solrQuery.set("cursorMark", cursorMark);
            } else {
                solrQuery.setStart(startIndex);
            }

            if (callback != null) {
                resp = solrServer.queryAndStreamResponse(solrQuery, callback);
            } else {
                resp = solrServer.query(solrQuery);
            }
        } catch (Exception exc) {

            //TODO
            //log.error("Query ["+solrQuery+"] failed due to: "+exc);

            // re-try once in the event of a communications error with the server
            Throwable rootCause = SolrException.getRootCause(exc);
            boolean wasCommError =
                    (rootCause instanceof ConnectException ||
                            rootCause instanceof IOException ||
                            rootCause instanceof SocketException);
            if (wasCommError) {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }

                try {
                    if (callback != null) {
                        resp = solrServer.queryAndStreamResponse(solrQuery, callback);
                    } else {
                        resp = solrServer.query(solrQuery);
                    }
                } catch (Exception excOnRetry) {
                    if (excOnRetry instanceof SolrServerException) {
                        throw (SolrServerException)excOnRetry;
                    } else {
                        throw new SolrServerException(excOnRetry);
                    }
                }
            } else {
                if (exc instanceof SolrServerException) {
                    throw (SolrServerException)exc;
                } else {
                    throw new SolrServerException(exc);
                }
            }
        }

        return resp;
    }

}
