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
package de.qaware.chronix.spark.api.java;

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import java.util.List;
import static de.qaware.chronix.spark.api.java.TestConfiguration.APP_NAME;
import static de.qaware.chronix.spark.api.java.TestConfiguration.SPARK_MASTER;
import static de.qaware.chronix.spark.api.java.TestConfiguration.ZK_HOST;

public class TestChronixSparkContext {

    @Ignore
    @Test
    public void testSolrQuery() {
        SparkConf conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);
        try(JavaSparkContext sc = new JavaSparkContext(conf)) {
            ChronixSparkContext csc = new ChronixSparkContext(sc);
            SolrQuery query = new SolrQuery("logMessage:*570*");
            JavaRDD<SolrDocument> result = csc.querySolr(query, ZK_HOST);
            List<SolrDocument> docs = result.take(1);
            Assert.assertTrue(docs.size() == 1);
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
    }

    @Ignore
    @Test
    public void testChronixQuery() {
        SparkConf conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);
        try(JavaSparkContext sc = new JavaSparkContext(conf)) {
            ChronixSparkContext csc = new ChronixSparkContext(sc);
            SolrQuery query = new SolrQuery("metric:\"MXBean(java.lang:type=Memory).NonHeapMemoryUsage.used\" AND type:RECORD");
            ChronixRDD result = csc.queryChronix(query, ZK_HOST);
            List<MetricTimeSeries> timeSeries = result.take(5);
            Assert.assertTrue(timeSeries.size() == 5);
            for (MetricTimeSeries ts : timeSeries) {
                System.out.println(ts.toString());
            }
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
    }
}
