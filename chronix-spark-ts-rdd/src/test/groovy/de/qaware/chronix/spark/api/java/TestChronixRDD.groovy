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

import de.qaware.chronix.spark.api.java.functions.FilterObservationByTimestamp
import de.qaware.chronix.storage.solr.timeseries.metric.MetricDimensions
import de.qaware.chronix.storage.solr.timeseries.metric.MetricObservation
import de.qaware.chronix.timeseries.MetricTimeSeries
import org.apache.solr.client.solrj.SolrQuery
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import spock.lang.Shared
import spock.lang.Specification

import static org.junit.Assert.assertTrue

class TestChronixRDD extends Specification {

    @Shared
    SparkConf conf
    @Shared
    JavaSparkContext sc
    @Shared
    ChronixSparkContext csc
    @Shared
    SolrQuery query
    @Shared
    ChronixRDD rdd
    @Shared
    SQLContext sqlContext

    def setup() {
        sc = SparkTestConfiguration.createSparkContext();
        csc = new ChronixSparkContext(sc);
        sqlContext = new SQLContext(sc);
        query = new SolrQuery(SparkTestConfiguration.SOLR_REFERENCE_QUERY);
        rdd = csc.query(query, SparkTestConfiguration.ZK_HOST, SparkTestConfiguration.CHRONIX_COLLECTION, SparkTestConfiguration.STORAGE);
    }

    def "test iterator"() {
        when:
        Iterator<MetricTimeSeries> it = rdd.iterator();
        int i = 0;
        while (it.hasNext()) {
            MetricTimeSeries mts = it.next();
            println mts
            i++;
        }
        then:
        i > 0
    }

    def "test mean"() {
        when:
        long start = System.currentTimeMillis();
        double mean = rdd.mean();
        long count = rdd.countObservations()
        long stop = System.currentTimeMillis();
        println "Mean: " + mean
        println "   - execution time: " + (stop - start)
        println "   - relevant observations: " + count
        then:
        mean > 0.0f
    }

    def "test approx. mean"() {
        when:
        long start = System.currentTimeMillis();
        double mean = rdd.approxMean()
        long stop = System.currentTimeMillis();
        println "Approx. mean: " + mean
        println "   - execution time: " + (stop - start)
        then:
        mean > 0.0f
    }

    def "test slopes / linear regression"() {
        when:
        List<Double> slopesList = rdd.getSlopes().collect();
        for (double slope : slopesList) {
            System.out.println("Slope: " + slope);
        }
        then:
        slopesList.size() > 0
    }

    def "test scalar actions"() {
        when:
        long count = rdd.countObservations()
        long countTs = rdd.count();
        double max = rdd.max()
        double min = rdd.min()
        println "Count observations: " + count
        println "Count time series: " + countTs
        println "Max: " + max
        println "Min: " + min
        then:
        count > 0
    }

    def "test data frame"() {
        given:
        DataFrame df = rdd.toDataFrame(sqlContext);
        when:
        df.show();
        then:
        //Assert that all columns are available
        List<String> columns = Arrays.asList(df.columns());
        assertTrue(columns.contains("timestamp"));
        assertTrue(columns.contains("value"));
        for (MetricDimensions dim : MetricDimensions.values()) {
            assertTrue(columns.contains(dim.getId()));
        }
        //Assert that DataFrame is not empty
        assertTrue(df.count() > 0);
    }

    def "test dataset"() {
        given:
        Dataset<MetricObservation> ds = rdd.toObservationsDataset(sqlContext);
        when:
        Dataset<MetricObservation> filteredDs = ds.filter(
                new FilterObservationByTimestamp(1458069519136L));
        then:
        assertTrue(filteredDs.count() > 0 && filteredDs.count() < ds.count());
    }

    def "test join"() {
        when:
        ChronixRDD cRdd = rdd.joinChunks()
        then:
        cRdd.count() == rdd.count()
    }

    def "test filter functions"() {
        given:
        Map<String, String> dimFilter = new HashMap<>()
        dimFilter.put("measurement", "20160317")
        dimFilter.put("group", "jenkins-jmx")
        when:
        ChronixRDD resRdd = rdd.filterByDimensions(dimFilter)
        then:
        resRdd.count() > 0
    }

}