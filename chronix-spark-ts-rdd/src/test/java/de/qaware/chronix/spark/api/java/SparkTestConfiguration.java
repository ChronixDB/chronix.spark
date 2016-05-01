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

import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.storage.solr.mock.ChronixSolrCloudStorageMock;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Some common configuration for the test suite.
 *
 */
public class SparkTestConfiguration {

    public enum Profile {
        IN_PROCESS,
        LOCAL
    }

    // INDIVIDUAL configuration **************************************************
    public static final String SPARK_HISTORY_DIR = "/tmp";
    public static final String[] JARS = new String[]{
            "/Users/qaware-jad/Documents/projekte/chronix/chronix.spark/chronix-solr-cloud-storage/build/libs/chronix-solr-cloud-storage-0.1.jar",
            "/Users/qaware-jad/Documents/projekte/chronix/chronix.spark/chronix-spark-ts-rdd/build/libs/chronix-spark-ts-rdd-0.1.jar"
    };

    // GLOBAL configuration ******************************************************
    public static final String APP_NAME = "Spark Chronix";
    public static final String SOLR_REFERENCE_QUERY = "metric:\"java.lang:type=Memory/HeapMemoryUsage/used\"";
    public static final String DEFAULT_TESTDATA_FILE = "timeseries-testdata.bin";
    public static final String CONFIG_PROPERTY = "CONFIG_PROFILE";

    // IN_PROCESS configuration **************************************************
    public static final String CHRONIX_COLLECTION_IN_PROCESS = "chronix";
    public static final String ZK_HOST_IN_PROCESS = "";
    public static final String SPARK_MASTER_IN_PROCESS = "local[4]";
    public static final ChronixSolrCloudStorage STORAGE_IN_PROCESS = new ChronixSolrCloudStorageMock(DEFAULT_TESTDATA_FILE);

    // LOCAL configuration ********************************************************
    public static final String CHRONIX_COLLECTION_LOCAL = "chronix";
    public static final String ZK_HOST_LOCAL = "localhost:9983";
    public static final String SPARK_MASTER_LOCAL = "spark://localhost:8987";
    public static final ChronixSolrCloudStorage STORAGE_LOCAL = new ChronixSolrCloudStorage();

    // Initialize the configuration space
    public static String CHRONIX_COLLECTION;
    public static String ZK_HOST;
    public static String SPARK_MASTER;
    public static ChronixSolrCloudStorage STORAGE;

    private static JavaSparkContext jsc;

    static {
        System.setProperty(CONFIG_PROPERTY, Profile.IN_PROCESS.name());
    }

    public static JavaSparkContext createSparkContext() {

        if (jsc != null) return jsc;

        if (System.getProperty(CONFIG_PROPERTY).equals(Profile.LOCAL.name())) {
            System.out.println("Activating profile LOCAL");
            CHRONIX_COLLECTION = CHRONIX_COLLECTION_LOCAL;
            ZK_HOST = ZK_HOST_LOCAL;
            SPARK_MASTER = SPARK_MASTER_LOCAL;
            STORAGE = STORAGE_LOCAL;
        } else {
            System.out.println("Activating profile IN_PROCESS");
            CHRONIX_COLLECTION = CHRONIX_COLLECTION_IN_PROCESS;
            ZK_HOST = ZK_HOST_IN_PROCESS;
            SPARK_MASTER = SPARK_MASTER_IN_PROCESS;
            STORAGE = STORAGE_IN_PROCESS;
        }

        SparkConf conf = new SparkConf()
                .setMaster(SparkTestConfiguration.SPARK_MASTER)
                .setAppName(SparkTestConfiguration.APP_NAME);
        ChronixSparkContext.tuneSparkConf(conf);
        conf.set("spark.driver.allowMultipleContexts", "true");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.eventLog.dir", SPARK_HISTORY_DIR);
        if (System.getProperty(CONFIG_PROPERTY).equals(Profile.LOCAL.name())) {
            conf.setJars(JARS);
        }
        jsc = new JavaSparkContext(conf);
        return jsc;
    }

}