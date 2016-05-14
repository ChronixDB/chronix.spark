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

import de.qaware.chronix.spark.api.java.config.ChronixSparkLoader
import de.qaware.chronix.storage.solr.timeseries.metric.MetricDimension
import de.qaware.chronix.storage.solr.timeseries.metric.MetricObservation
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries
import org.apache.spark.api.java.JavaDoubleRDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import spock.lang.Shared
import spock.lang.Specification

import static org.junit.Assert.assertTrue

class TestChronixRDD extends Specification {

    @Shared
    ChronixSparkLoader loader
    @Shared
    ChronixSparkContext csc
    @Shared
    ChronixRDD rdd
    @Shared
    SQLContext sqlContext

    def setup() {
        loader = new ChronixSparkLoader();
        csc = loader.createChronixSparkContext();
        sqlContext = new SQLContext(csc.getSparkContext());
        rdd = loader.createChronixRDD(csc);
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

    def "get values as rdd"() {
        when:
        JavaDoubleRDD rdd = rdd.getValuesAsRdd()
        then:
        double mean = rdd.mean();
        println "Mean: " + mean
        rdd.count() > 0
    }

    def "test data frame"() {
        when:
        DataFrame df = rdd.toDataFrame(sqlContext);
        then:
        //Assert that all columns are available
        List<String> columns = Arrays.asList(df.columns());
        assertTrue(columns.contains("timestamp"));
        assertTrue(columns.contains("value"));
        for (MetricDimension dim : MetricDimension.values()) {
            assertTrue(columns.contains(dim.getId()));
        }
        //Assert that DataFrame is not empty
        df.show()
        assertTrue(df.count() > 0);

        DataFrame dfRes = df.select("value", "process").where("process=\"jenkins-jolokia\"")
        dfRes.show()
        assertTrue(dfRes.count() > 0)
    }

    def "test dataset"() {
        when:
        Dataset<MetricObservation> ds = rdd.toObservationsDataset(sqlContext);
        then:
        assertTrue(ds.count() > 0);
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
        dimFilter.put(MetricDimension.MEASUREMENT_SERIES, "20160317")
        dimFilter.put(MetricDimension.METRIC_GROUP, "jenkins-jmx")
        when:
        ChronixRDD resRdd = rdd.filterByDimensions(dimFilter)
        then:
        resRdd.count() > 0
    }

}