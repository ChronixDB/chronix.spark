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
import spock.lang.Ignore
import spock.lang.Specification

class TestChronixRDD extends Specification {

    @Ignore
    def "test iterator"() {
        given:
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf)
        ChronixSparkContext csc = new ChronixSparkContext(sc);
        SolrQuery query = new SolrQuery("metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\" AND type:RECORD");
        ChronixRDD rdd = csc.queryChronix(query, ConfigurationParams.ZK_HOST);
        when:
        Iterator<MetricTimeSeries> it = rdd.iterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            i++;
        }
        then:
        i > 0
        cleanup:
        sc.close()
    }

    @Ignore
    def "test mean"() {
        given:
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf)
        ChronixSparkContext csc = new ChronixSparkContext(sc);
        SolrQuery query = new SolrQuery("metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\" AND type:RECORD");
        ChronixRDD rdd = csc.queryChronix(query, ConfigurationParams.ZK_HOST);
        when:
        long start = System.currentTimeMillis();
        double mean = rdd.mean();
        long stop = System.currentTimeMillis();
        println "Mean: " + mean
        println "   - execution time: " + (stop - start)
        then:
        mean > 0.0f
        cleanup:
        sc.close()
    }

    @Ignore
    def "test approx. mean"() {
        given:
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf)
        ChronixSparkContext csc = new ChronixSparkContext(sc);
        SolrQuery query = new SolrQuery("metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\" AND type:RECORD");
        ChronixRDD rdd = csc.queryChronix(query, ConfigurationParams.ZK_HOST);
        when:
        long start = System.currentTimeMillis();
        double mean = rdd.approxMean()
        long stop = System.currentTimeMillis();
        println "Approx. mean: " + mean
        println "   - execution time: " + (stop - start)
        then:
        mean > 0.0f
        cleanup:
        sc.close()
    }

    @Ignore
    def "test scalar actions"() {
        given:
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf)
        ChronixSparkContext csc = new ChronixSparkContext(sc);
        SolrQuery query = new SolrQuery("metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\" AND type:RECORD");
        ChronixRDD rdd = csc.queryChronix(query, ConfigurationParams.ZK_HOST);
        when:
        long count = rdd.countObservations()
        double max = rdd.max()
        double min = rdd.min()
        println "Count: " + count
        println "Max: " + max
        println "Min: " + min
        then:
        count > 0
        cleanup:
        sc.close()
    }

}
