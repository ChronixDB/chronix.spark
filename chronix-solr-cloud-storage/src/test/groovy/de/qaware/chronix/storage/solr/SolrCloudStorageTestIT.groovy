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
package de.qaware.chronix.storage.solr

import de.qaware.chronix.storage.solr.converter.MetricTimeSeriesConverter
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import spock.lang.Ignore
import spock.lang.Specification

import java.util.stream.Collectors

/**
 * Integration test for the solr cloud storage
 * @author f.lautenschlager
 */
class SolrCloudStorageTestIT extends Specification {

    @Ignore
    def "test can connect to solr cloud"() {
        given:
        def solrCloudStorage = new ChronixSolrCloudStorage(200)
        def converter = new MetricTimeSeriesConverter()
        def connection = new CloudSolrClient("192.168.1.100:2181")
        connection.setDefaultCollection("ekgdata")
        def query = new SolrQuery("metric:\"MXBean(com.bea:Name=SDWHDataSource,ServerRuntime=i1_lpswl11,Type=JDBCDataSourceRuntime).PrepStmtCacheMissCount\"");
        when:
        def stream = solrCloudStorage.stream(converter, connection, query)

        then:
        List result = stream.collect(Collectors.toList());
        println "Result has a length of ${result.size()}"

    }

}
