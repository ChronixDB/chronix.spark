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

import de.qaware.chronix.spark.api.java.timeseries.MetricObservation;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertTrue;

/**
 * Tests the (experimental) Dataset API exposure of Chronix Spark.
 */
public class TestDataset implements Serializable {

    private static final long serialVersionUID = 42L;

    @Test
    public void testDatasetCreationWithObservations() throws SolrServerException {
        //Create Spark context
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            //Create Chronix Spark context
            ChronixSparkContext csc = new ChronixSparkContext(sc);

            //Create Spark SQL Context
            SQLContext sqlContext = new SQLContext(sc);

            //Read data into ChronixRDD
            SolrQuery query = new SolrQuery(ConfigurationParams.SOLR_REFERNCE_QUERY);
            ChronixRDD rdd = csc.queryChronixChunks(query,
                    ConfigurationParams.ZK_HOST,
                    ConfigurationParams.CHRONIX_COLLECTION,
                    ConfigurationParams.STORAGE);

            //Transform ChronixRDD to Dataset
            Dataset<MetricObservation> ds = rdd.toObservationsDataset(sqlContext);
            ds.cache();
            Dataset<MetricObservation> filteredDs = ds.filter(new FilterFunction<MetricObservation>() {
                @Override
                public boolean call(MetricObservation value) throws Exception {
                    return value.getTimestamp() == 1458069519136L;
                }
            });
            filteredDs.cache();
            assertTrue(filteredDs.count() > 0 && filteredDs.count() < ds.count());
        } finally {
            //Clean up
            sc.close();
        }
    }

}
