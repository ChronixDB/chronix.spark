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
package de.qaware.chronix.storage.solr.converter;

import de.qaware.chronix.Schema;
import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.dts.Pair;
import de.qaware.chronix.timeseries.TimeSeries;
import de.qaware.ekg.collector.storage.protocol.ProtocolBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * Converter for Chronix to convert
 *
 * @author f.lautenschlager
 * @author l.buchner
 */
public class MetricTimeSeriesConverter implements TimeSeriesConverter<TimeSeries<Long, Double>> {

    private final Logger LOGGER = LoggerFactory.getLogger(MetricTimeSeriesConverter.class);

    @Override
    public TimeSeries<Long, Double> from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        try {

            String blob = (String) binaryTimeSeries.get(Schema.DATA);
            ProtocolBuffer.Points protocolBufferPoints = Util.loadIntoProtocolBuffers(blob);

            Iterator<Pair<Long, Double>> iter = protocolBufferPoints.getPointsList().stream().map(p -> Pair.pairOf(p.getDate(), p.getValue())).iterator();
            TimeSeries<Long, Double> timeSeries = new TimeSeries<>(iter);

            binaryTimeSeries.getFields().forEach((field, value) -> {
                if (Schema.isUserDefined(field)) {
                    //Add attributes
                    timeSeries.addAttribute(field, value);
                }
            });

            //convert start and end to long
            Date start = (Date) binaryTimeSeries.get(Schema.START);
            Date end = (Date) binaryTimeSeries.get(Schema.END);
            timeSeries.addAttribute(Schema.START, start.getTime());
            timeSeries.addAttribute(Schema.END, end.getTime());

            return timeSeries;

        } catch (IOException e) {
            LOGGER.error("Could not convert binary time series to metric time series.", e);
        }
        return null;
    }

    @Override
    public BinaryTimeSeries to(TimeSeries<Long, Double> document) {
        BinaryTimeSeries.Builder binaryTimeSeries = new BinaryTimeSeries.Builder();

        //Add attributes
        Iterator<Map.Entry<String, Object>> it = document.getAttributes();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            binaryTimeSeries.field(entry.getKey(), entry.getValue());
        }

        //Add points
        ProtocolBuffer.Points.Builder builder = ProtocolBuffer.Points.newBuilder();
        ProtocolBuffer.Point.Builder pointBuilder = ProtocolBuffer.Point.newBuilder();
        for (Pair<Long, Double> point : document) {
            if (point.getFirst() != null) {
                builder.addPoints(pointBuilder.setDate(point.getFirst()).setValue(point.getSecond()));
            }
        }

        //add the data field to the binary time series
        String data = Util.fromProtocolBuffersToChunk(builder.build());
        binaryTimeSeries.field(Schema.DATA, data);

        return binaryTimeSeries.build();
    }
}

