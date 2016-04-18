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
import de.qaware.chronix.timeseries.dt.Point;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

/**
 * Perform a SAX analysis on chunked time series data.
 */
public class TestISAXAnalysis implements Serializable {

    /**
     * Uses a symbolic representation of a time series (see http://www.cs.ucr.edu/~eamonn/iSAX.pdf)
     *
     * @throws SolrServerException
     */
    @Test
    public void performISAXAnalysis() throws SolrServerException {
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);
        ChronixSparkContext csc = new ChronixSparkContext(sc);
        SolrQuery query = new SolrQuery(ConfigurationParams.SOLR_REFERNCE_QUERY);
        ChronixRDD rdd = csc.queryChronixChunks(query, ConfigurationParams.ZK_HOST, ConfigurationParams.CHRONIX_COLLECTION, ConfigurationParams.STORAGE);
        JavaRDD<Double> slopes = rdd.map(new Function<MetricTimeSeries, Double>() {

                    @Override
                    public Double call(MetricTimeSeries mts) throws Exception {
                        SimpleRegression regression = new SimpleRegression();
                        mts.points().forEach( p -> {
                            regression.addData(p.getTimestamp(), p.getValue());
                        });
                        return regression.getSlope();
                    }
                }
        );
        List<Double> slopesList = slopes.collect();
        for (double slope : slopesList) {
            System.out.println("Slope: " + slope);
        }
    }
}
