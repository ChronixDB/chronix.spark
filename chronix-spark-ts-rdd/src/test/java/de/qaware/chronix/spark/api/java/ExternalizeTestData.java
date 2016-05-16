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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import de.qaware.chronix.spark.api.java.config.ChronixSparkLoader;
import de.qaware.chronix.spark.api.java.config.ChronixYAMLConfiguration;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries;
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
 * Externalizes data from a given Chronix
 * Solr Cloud Storage for offline usage. Mainly
 * used to create test data containers.
 */
public class ExternalizeTestData {

    /**
     * @param args optional first argument: file to serialize to. A default file name is provided.
     * @throws SolrServerException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws SolrServerException, IOException {

        ChronixSparkLoader chronixSparkLoader = new ChronixSparkLoader();
        ChronixYAMLConfiguration config = chronixSparkLoader.getConfig();

        String file = (args.length >= 1)
                ? args[0]
                : config.getTestdataFile();

        Path filePath = Paths.get(file);
        Files.deleteIfExists(filePath);
        Output output = new Output(new DeflaterOutputStream(new FileOutputStream(filePath.toString())));
        System.out.println("Opening test data file: " + filePath.toString());
        
        ChronixSparkContext cSparkContext = null;

        //Create target file
        try {
            //Create Chronix Spark context
            cSparkContext = chronixSparkLoader.createChronixSparkContext();
            
            //Read data into ChronixRDD
            SolrQuery query = new SolrQuery(config.getSolrReferenceQuery());
            ChronixRDD rdd = cSparkContext.queryChronixChunks(query,
                    config.getZookeeperHost(),
                    config.getChronixCollection(),
                    config.getStorage());

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
            if (cSparkContext != null) {
                cSparkContext.getSparkContext().close();
            }
            System.out.println("Test data file written successfully!");
        }
    }

}
