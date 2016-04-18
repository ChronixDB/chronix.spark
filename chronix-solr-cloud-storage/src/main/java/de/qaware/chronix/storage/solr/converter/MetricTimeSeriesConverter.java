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
import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.ekg.collector.storage.protocol.ProtocolBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 *
 */
public class MetricTimeSeriesConverter implements TimeSeriesConverter<MetricTimeSeries> {

    private static final String METRIC = "metric";
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricTimeSeriesConverter.class);

    @Override
    public MetricTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        try {
            //get the metric
            if (binaryTimeSeries.get(METRIC) == null) {
                //Should not happen
                throw new IllegalStateException("");
            }
            String metric = binaryTimeSeries.get(METRIC).toString();

            MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metric);

            String blob = (String) binaryTimeSeries.get(Schema.DATA);
            ProtocolBuffer.Points protocolBufferPoints = Util.loadIntoProtocolBuffers(blob);

            for (ProtocolBuffer.Point point : protocolBufferPoints.getPointsList()) {
                if (point.getDate() >= queryStart && point.getDate() <= queryEnd) {
                    tsBuilder.point(point.getDate(), point.getValue());
                }
            }

            binaryTimeSeries.getFields().forEach((field, value) -> {
                if (Schema.isUserDefined(field)) {
                    //Add attributes
                    tsBuilder.attribute(field, value);
                }
            });

            //convert start and end to long
            Date start = (Date) binaryTimeSeries.get(Schema.START);
            Date end = (Date) binaryTimeSeries.get(Schema.END);
            tsBuilder.attribute(Schema.START, start.getTime());
            tsBuilder.attribute(Schema.END, end.getTime());

            return tsBuilder.build();

        } catch (IOException e) {
            LOGGER.error("Could not convert binary time series to metric time series.", e);
        }
        return null;
    }

    @Override
    public BinaryTimeSeries to(MetricTimeSeries document) {
        BinaryTimeSeries.Builder binaryTimeSeries = new BinaryTimeSeries.Builder();

        //Add attributes
        Map<String, Object> attributes = document.getAttributesReference();
        for (Map.Entry<String, Object> attribute : attributes.entrySet()) {
            binaryTimeSeries.field(attribute.getKey(), attribute.getValue());
        }

        //Add points
        ProtocolBuffer.Points.Builder builder = ProtocolBuffer.Points.newBuilder();
        ProtocolBuffer.Point.Builder pointBuilder = ProtocolBuffer.Point.newBuilder();
        document.points().forEach(point -> builder.addPoints(pointBuilder.setDate(point.getTimestamp()).setValue(point.getValue())));

        //add the data field to the binary time series
        String data = Util.fromProtocolBuffersToChunk(builder.build());
        binaryTimeSeries.field(Schema.DATA, data);

        return binaryTimeSeries.build();
    }
}
