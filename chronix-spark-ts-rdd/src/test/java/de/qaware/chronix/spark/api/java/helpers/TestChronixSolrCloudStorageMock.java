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

import de.qaware.chronix.spark.api.java.ConfigurationParams;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.junit.Test;

import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

public class TestChronixSolrCloudStorageMock {

    @Test
    public void testMock() {
        String file = this.getClass().getClassLoader().getResource(ConfigurationParams.DEFAULT_TESTDATA_FILE).getFile();
        System.out.println("Reading file: " + file);
        ChronixSolrCloudStorageMock mock = new ChronixSolrCloudStorageMock(file);
        Stream<MetricTimeSeries> mtsStream = mock.streamFromSingleNode(null, null, null);
        long entries = mtsStream.count();
        System.out.println("Number of test time series:" + entries);
        assertTrue(entries > 0);
    }

}
