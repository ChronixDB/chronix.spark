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

import de.qaware.chronix.converter.KassiopeiaSimpleConverter;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ChronixSolrCloudStorageTest {

    public static final String ZK_HOST = "localhost:9983";
    public static final String COLLECTION = "chronix";
    public static final String SOLR_REFERENCE_QUERY = "metric:\"java.lang:type=Memory/HeapMemoryUsage/used\"";

    @Test
    /**
     * manual URL:
     * http://localhost:8983/solr/chronix_shard1_replica1/export?q=*:*&fl=metric&sort=id+asc
     */
    public void testStreamTimeSeries() throws IOException {
        ChronixSolrCloudStorage cs = new ChronixSolrCloudStorage();
        //cs.switchToSolrTupleExport();
        List<String> shards = cs.getShardList(ZK_HOST, COLLECTION);
        for (String shard : shards) {
            Stream<MetricTimeSeries> mtss = cs.streamFromSingleNode(ZK_HOST, COLLECTION, shard, new SolrQuery(SOLR_REFERENCE_QUERY), new KassiopeiaSimpleConverter());
            mtss.collect(Collectors.toList()).forEach(System.out::println);
        }

    }

}
