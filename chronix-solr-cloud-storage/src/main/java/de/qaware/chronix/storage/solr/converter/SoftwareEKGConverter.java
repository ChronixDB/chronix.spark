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
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.zip.GZIPInputStream;

/**
 *
 */
public class SoftwareEKGConverter implements TimeSeriesConverter<MetricTimeSeries> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SoftwareEKGConverter.class);
    public static final String METRIC = "metric";

    @Override
    public MetricTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        try {
            //get the metric
            String metric = defaultIfNull(binaryTimeSeries.get(METRIC).toString(), "default");

            MetricTimeSeries.Builder tsBuilder = new MetricTimeSeries.Builder(metric);

            // Decode, Decompress, Deserialize
            String blob = (String) binaryTimeSeries.get(Schema.DATA);
            byte[] decoded = Base64.decodeBase64(blob);
            ProtocolBuffer.Points protcolBufferPoints = ProtocolBuffer.Points.parseFrom(new GZIPInputStream(new ByteArrayInputStream(decoded)));

            for (ProtocolBuffer.Point point : protcolBufferPoints.getPointsList()) {
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

    private <T> T defaultIfNull(T metric, T aDefault) {
        if (metric == null) {
            return aDefault;
        }
        return metric;
    }

    @Override
    public BinaryTimeSeries to(MetricTimeSeries document) {
        return new BinaryTimeSeries.Builder().build();
    }
}
