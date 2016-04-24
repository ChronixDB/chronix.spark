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
package de.qaware.chronix.spark.api.java

import de.qaware.chronix.timeseries.MetricTimeSeries
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.junit.Assert
import spock.lang.Shared
import spock.lang.Specification

class TestChronixSparkContext extends Specification {

    @Shared
    SparkConf conf
    @Shared
    JavaSparkContext sc
    @Shared
    ChronixSparkContext csc
    @Shared
    SolrQuery query

    def setup() {
        sc = SparkConfiguration.createSparkContext();
        csc = new ChronixSparkContext(sc);
        query = new SolrQuery(SparkConfiguration.SOLR_REFERNCE_QUERY)
    }

    def "testChronixQuery"() {
        when:
        ChronixRDD result = csc.queryChronixChunks(query, SparkConfiguration.ZK_HOST, SparkConfiguration.CHRONIX_COLLECTION, SparkConfiguration.STORAGE)
        then:
        List<MetricTimeSeries> timeSeries = result.take(5)

        then:
        Assert.assertTrue(timeSeries.size() == 5)
        for (MetricTimeSeries ts : timeSeries) {
            System.out.println(ts.toString());
        }
        cleanup:
        sc.close()
    }

    def "testQuery"() {
        when:
        ChronixRDD resultChunked = csc.queryChronixChunks(query, SparkConfiguration.ZK_HOST, SparkConfiguration.CHRONIX_COLLECTION, SparkConfiguration.STORAGE)
        ChronixRDD result = csc.query(query, SparkConfiguration.ZK_HOST, SparkConfiguration.CHRONIX_COLLECTION, SparkConfiguration.STORAGE)
        then:
        long chunked = resultChunked.count()
        long joined = result.count()
        println "Chunked: " + chunked
        println "Joined: " + joined
        for (MetricTimeSeries mts : result.collect()) {
            println mts
            //TODO: check ordering in time series
            //TODO: check duplicates according time series identity (and eliminate the assert below which is fragile)
        }
        Assert.assertTrue(resultChunked.count() >= result.count())
        cleanup:
        sc.close()
    }
}
