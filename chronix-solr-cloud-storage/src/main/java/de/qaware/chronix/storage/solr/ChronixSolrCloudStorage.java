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
import de.qaware.chronix.streaming.StorageService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @param <T> the type defining the returned type
 */
public class ChronixSolrCloudStorage<T> implements StorageService<T, CloudSolrClient, SolrQuery>, Serializable {

    private static final long serialVersionUID = 42L;

    private final int nrOfDocumentPerBatch;


    /**
     * Constructs a Chronix storage that is based on an apache solr cloud.
     *
     * @param nrOfDocumentPerBatch number of documents that are processed in one batch
     */
    public ChronixSolrCloudStorage(final int nrOfDocumentPerBatch) {
        this.nrOfDocumentPerBatch = nrOfDocumentPerBatch;
    }

    @Override
    public Stream<T> stream(TimeSeriesConverter<T> converter, CloudSolrClient connection, SolrQuery query) {
        LoggerFactory.getLogger(ChronixSolrCloudStorage.class).debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, connection, query);
        SolrStreamingService<T> solrStreamingService = new SolrStreamingService<>(converter, query, connection, nrOfDocumentPerBatch);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false);
    }

    /**
     * Fetches a stream of time series only from a single node.
     *
     * @param converter  the time series type converter
     * @param connection the solr client connection to a single solr server
     * @param query      the solr query
     * @return a stream of time series
     */
    public Stream<T> streamFromSingleNode(TimeSeriesConverter<T> converter, SolrClient connection, SolrQuery query) {
        LoggerFactory.getLogger(ChronixSolrCloudStorage.class).debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, connection, query);
        SolrStreamingService<T> solrStreamingService = new SolrStreamingService<>(converter, query, connection, nrOfDocumentPerBatch);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false);
    }


    @Override
    public boolean add(TimeSeriesConverter<T> converter, Collection<T> documents, CloudSolrClient connection) {
        throw new UnsupportedOperationException();
    }
}
