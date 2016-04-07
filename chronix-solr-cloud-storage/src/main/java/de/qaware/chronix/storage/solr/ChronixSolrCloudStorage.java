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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

/**
 * @param <T> the type defining the returned type
 */
public class ChronixSolrCloudStorage<T> implements StorageService<T, CloudSolrClient, SolrQuery> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChronixSolrCloudStorage.class);
    private final int nrOfDocumentPerBatch;
    private final BinaryOperator<T> reduce;
    private final Function<T, String> groupBy;

    /**
     * Constructs a Chronix storage that is based on an apache solr cloud.
     *
     * @param nrOfDocumentPerBatch number of documents that are processed in one batch
     * @param groupBy              the function to group time series records
     * @param reduce               the function to reduce the grouped time series records into one time series
     */
    public ChronixSolrCloudStorage(final int nrOfDocumentPerBatch, final Function<T, String> groupBy, final BinaryOperator<T> reduce) {
        this.nrOfDocumentPerBatch = nrOfDocumentPerBatch;
        this.groupBy = groupBy;
        this.reduce = reduce;
    }

    @Override
    public Stream<T> stream(TimeSeriesConverter<T> converter, CloudSolrClient connection, SolrQuery query) {
        LOGGER.debug("Streaming data from solr using converter {}, Solr Client {}, and Solr Query {}", converter, connection, query);
        SolrStreamingService<T> solrStreamingService = new SolrStreamingService<>(converter, query, connection, nrOfDocumentPerBatch);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(solrStreamingService, Spliterator.SIZED), false)
                .filter(t -> t != null)//Remove empty results
                .collect(groupingBy((Function<T, String>) groupBy::apply)).values().stream()
                .map(ts -> ts.stream().reduce(reduce).get());
    }


    @Override
    public boolean add(TimeSeriesConverter<T> converter, Collection<T> documents, CloudSolrClient connection) {
        //TODO: Implement
        //Read only.
        return true;
    }
}
