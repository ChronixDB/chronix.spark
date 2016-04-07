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
 * Unit test for the kassiopeia time series converter
 * @author f.lautenschlager
 */
class KassiopeiaTimeSeriesConverterTest extends Specification {

    def "test convert kassiopeia time series"() {
        given:
        def binaryTS = new BinaryTimeSeries.Builder()
                .field("test", 6d)
                .field(Schema.DATA, compressedData())
                .field(Schema.END, new Date())
                .field(Schema.START, new Date())
                .build()

        when:
        def timeSeries = new KassiopeiaTimeSeriesConverter().from(binaryTS, 0, 0)

        then:
        timeSeries.size() == 11
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
