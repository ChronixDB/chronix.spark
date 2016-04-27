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

import de.qaware.chronix.spark.api.java.timeseries.metric.MetricObservation;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.Iterator;


/**
 * Single test suite for performance regression testing
 */
public class TestPerformanceRegression {

    private static final long LOOPS = 2;

    /**
     * ----------------------------------------------------
     * OPTIMIZATION LOG
     * Please add your new measurements here with
     * every change on Spark Chronix.
     * ----------------------------------------------------
     * first attempt                              : 9,603ms
     * with rdd.cache()                           : 8,223ms
     * with binary protocol & rdd compression     : 7,561ms
     * with binary protocol                       : 6,865ms
     * with kryo                                  : 6,347ms
     * with lzf compression                       : 6,012ms
     */
    public static void main(String[] args) throws SolrServerException, IOException {
        //Create Spark context
        JavaSparkContext sc = SparkTestConfiguration.createSparkContext();

        try {
            //Create Chronix Spark context
            ChronixSparkContext csc = new ChronixSparkContext(sc);

            //Create Spark SQL Context
            SQLContext sqlContext = new SQLContext(sc);


            // BENCHMARK START ...............................
            long start = System.currentTimeMillis();
            for (int i = 0; i < LOOPS; i++) {

                //Read data into ChronixRDD
                SolrQuery query = new SolrQuery(SparkTestConfiguration.SOLR_REFERNCE_QUERY);
                ChronixRDD rdd = csc.queryChronixChunks(query,
                        SparkTestConfiguration.ZK_HOST,
                        SparkTestConfiguration.CHRONIX_COLLECTION,
                        new ChronixSolrCloudStorage());
                rdd.cache();

                //Some actions
                double mean = rdd.mean();
                double approxMean = rdd.approxMean();
                long observationCount = rdd.countObservations();
                double max = rdd.max();
                double min = rdd.min();
                Iterator<MetricTimeSeries> it = rdd.iterator();
                while (it.hasNext()) {
                    MetricTimeSeries mts = it.next();
                    System.out.print(".");
                }

                //DataFrame operations
                Dataset<MetricObservation> ds = rdd.toObservationsDataset(sqlContext);
                ds.count();
            }
            long stop = System.currentTimeMillis();
            // BENCHMARK STOP ...................................
            System.out.println("\nBenchmark duration: " + (stop - start) + " ms");
        } finally {
            //Clean up
            sc.close();
        }
    }

}
