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
package de.qaware.chronix.spark.api.java.helpers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import de.qaware.chronix.spark.api.java.ChronixRDD;
import de.qaware.chronix.spark.api.java.ChronixSparkContext;
import de.qaware.chronix.spark.api.java.ConfigurationParams;
import de.qaware.chronix.spark.api.java.util.SolrCloudUtil;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.commons.collections.IteratorUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.DeflaterOutputStream;

/**
 * Externalizes test data from a given Chronix Solr Cloud Storage
 */
public class DataExternalizer {

    /**
     * @param args optional first argument: file to serialize to. A default file name is provided.
     * @throws SolrServerException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws SolrServerException, IOException {

        String file = ConfigurationParams.DEFAULT_TESTDATA_FILE;
        if (args.length >= 1) file = args[0];

        Path filePath = Paths.get(file);
        Files.deleteIfExists(filePath);
        Output output = new Output(new DeflaterOutputStream(new FileOutputStream(filePath.toString())));
        System.out.println("Opening test data file: " + filePath.toString());

        //Create Spark context
        SparkConf conf = new SparkConf().setMaster(ConfigurationParams.SPARK_MASTER).setAppName(ConfigurationParams.APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Create target file
        try {
            //Create Chronix Spark context
            ChronixSparkContext csc = new ChronixSparkContext(sc);

            //Read data into ChronixRDD
            SolrQuery query = new SolrQuery(ConfigurationParams.SOLR_REFERNCE_QUERY);
            ChronixRDD rdd = csc.queryChronix(query,
                    ConfigurationParams.ZK_HOST,
                    ConfigurationParams.CHRONIX_COLLECTION,
                    new ChronixSolrCloudStorage<MetricTimeSeries>(SolrCloudUtil.CHRONIX_DEFAULT_PAGESIZE));
            System.out.println("Writing " + rdd.count() + " time series into test data file.");

            //Loop through result and serialize it to disk
            Kryo kryo = new Kryo();
            List<MetricTimeSeries> mtsList = IteratorUtils.toList(rdd.iterator());
            System.out.println("Writing objects...");
            kryo.writeObject(output, mtsList);
            output.flush();
            System.out.println("Objects written.");
        } finally {
            output.close();
            sc.close();
            System.out.println("Test data file written successfully!");
        }
    }

}
