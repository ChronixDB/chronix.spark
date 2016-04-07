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
package de.qaware.chronix.storage.solr.converter


import de.qaware.chronix.Schema
import de.qaware.chronix.converter.BinaryTimeSeries
import de.qaware.chronix.converter.common.Compression
import de.qaware.ekg.collector.storage.protocol.ProtocolBuffer
import org.apache.commons.codec.binary.Base64
import spock.lang.Specification

/**
 * Unit test for the metric time series converter
 */
class MetricTimeSeriesConverterTest extends Specification {

    def "test convert metric time series"() {
        given:
        def start = new Date()
        def end = new Date()
        def binaryTS = new BinaryTimeSeries.Builder()
                .field("test", 6d)
                .field("metric", "random")
                .field(Schema.DATA, compressedData())
                .field(Schema.END, end)
                .field(Schema.START, start)
                .build()

        def metricConverter = new MetricTimeSeriesConverter()

        when:
        def metricTimeSeries = metricConverter.from(binaryTS, 0, 0)
        def testedTS = metricConverter.to(metricTimeSeries)

        then:
        testedTS.get("test") == 6d
        testedTS.start == start.time
        testedTS.end == end.time
    }

    String compressedData() {
        def points = ProtocolBuffer.Points.newBuilder()
        10.times {
            points.addPoints(ProtocolBuffer.Point.newBuilder().setDate(it).setValue(it).build())
        }
        def compressed = Compression.compress(points.build().toByteArray());

        new String(Base64.encodeBase64(compressed));
    }
}
