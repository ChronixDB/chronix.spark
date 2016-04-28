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
package de.qaware.chronix.storage.solr.mock;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.storage.solr.ChronixSolrCloudStorage;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.InflaterInputStream;

/**
 * A mock implementation of a Chronix Solr Cloud Storage.
 * <p>
 * Reads test data from a Kryo serialized file of time series test data.
 */
public class ChronixSolrCloudStorageMock extends ChronixSolrCloudStorage<MetricTimeSeries> {

    private String testDataFile;

    /**
     * @param testDataFileName file where the test data is. Use TestDataExternalizer to extract test data into a file.
     */
    public ChronixSolrCloudStorageMock(String testDataFileName) {
        super(1);
        this.testDataFile = testDataFileName;
    }

    @Override
    public Stream<MetricTimeSeries> stream(TimeSeriesConverter<MetricTimeSeries> converter, String zkHost, SolrQuery query) {
        return readTestData();
    }

    @Override
    public Stream<MetricTimeSeries> streamFromSingleNode(TimeSeriesConverter<MetricTimeSeries> converter, String shardUrl, SolrQuery query) {
        return readTestData();
    }

    /**
     * @return a stream of time series containing test data
     */
    private Stream<MetricTimeSeries> readTestData() {
        Kryo kryo = new Kryo();
        try {
            String file = this.getClass().getClassLoader().getResource(testDataFile).getFile();
            Input input = new Input(new InflaterInputStream(new FileInputStream(file)));
            List<MetricTimeSeries> mtsList = kryo.readObject(input, ArrayList.class);
            return mtsList.stream();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean add(TimeSeriesConverter<MetricTimeSeries> converter, Collection<MetricTimeSeries> documents, CloudSolrClient connection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getShardList(String zkHost, String chronixCollection) {
        List<String> shards = new ArrayList<String>(1);
        shards.add("http://dummy.solr/ONLY_ONE_TEST_SHARD");
        return shards;
    }

}