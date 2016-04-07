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

import de.qaware.chronix.storage.solr.converter.SoftwareEKGConverter
import de.qaware.chronix.timeseries.MetricTimeSeries
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.BinaryOperator
import java.util.function.Function
import java.util.stream.Collectors

/**
 * Integration test for the solr cloud storage
 * @author f.lautenschlager
 */
class SolrCloudStorageTestIT extends Specification {

    @Shared
    Function<MetricTimeSeries, String> groupBy = new Function<MetricTimeSeries, String>() {
        @Override
        String apply(MetricTimeSeries metricTimeSeries) {
            return metricTimeSeries.getMetric();
        }
    }

    @Shared
    BinaryOperator<MetricTimeSeries> reduce = new BinaryOperator<MetricTimeSeries>() {
        @Override
        MetricTimeSeries apply(MetricTimeSeries ts1, MetricTimeSeries ts2) {
            return ts1.addAll(ts2.getTimestampsAsArray(), ts2.getValuesAsArray());
        }
    }

    def "test can connect to solr cloud"() {
        given:
        def solrCloudStorage = new ChronixSolrCloudStorage(200, groupBy, reduce)
        def converter = new SoftwareEKGConverter()
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
