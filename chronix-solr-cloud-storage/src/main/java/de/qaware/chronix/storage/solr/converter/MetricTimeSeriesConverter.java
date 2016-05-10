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


import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.converter.common.MetricTSSchema;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricDimension;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The kassiopeia time series converter for the simple time series class
 *
 * @author f.lautenschlager
 */
public class MetricTimeSeriesConverter implements TimeSeriesConverter<MetricTimeSeries> {


    public static final String DATA_AS_JSON_FIELD = "dataAsJson";
    public static final String DATA_FUNCTION_VALUE = "function_value";

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricTimeSeriesConverter.class);

    @Override
    public MetricTimeSeries from(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd) {
        LOGGER.debug("Converting {} to MetricTimeSeries starting at {} and ending at {}", binaryTimeSeries, queryStart, queryEnd);
        //get the metric
        String metric = binaryTimeSeries.get(MetricTSSchema.METRIC).toString();

        //Third build a minimal time series
        MetricTimeSeries.Builder builder = new MetricTimeSeries.Builder(metric);

        //add all user defined attributes
        binaryTimeSeries.getFields().forEach((field, value) -> {
            if (MetricTSSchema.isUserDefined(field)) {
                MetricDimension dim = MetricDimension.getById(field);
                if (dim != null) { //ignore non-dimensional attributes
                    builder.attribute(dim, value.toString());
                }
            }
        });

        //Default serialization is protocol buffers.
        if (binaryTimeSeries.getPoints().length > 0) {
            fromProtocolBuffers(binaryTimeSeries, queryStart, queryEnd, builder);

        } else if (binaryTimeSeries.getFields().containsKey(DATA_AS_JSON_FIELD)) {
            //do it from json
            fromJson(binaryTimeSeries, queryStart, queryEnd, builder);

        } else if (binaryTimeSeries.get(DATA_FUNCTION_VALUE) != null) {
            //we have a function (aggregation) result
            double value = Double.valueOf(binaryTimeSeries.get(DATA_FUNCTION_VALUE).toString());
            long meanDate = meanDate(binaryTimeSeries);
            builder.point(meanDate, value);
        }

        return builder.build();
    }

    private void fromProtocolBuffers(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd, MetricTimeSeries.Builder builder) {
        ProtoBufMetricTimeSeriesSerializer.from(binaryTimeSeries.getPoints(), binaryTimeSeries.getStart(), binaryTimeSeries.getEnd(), queryStart, queryEnd, builder);
    }

    private void fromJson(BinaryTimeSeries binaryTimeSeries, long queryStart, long queryEnd, MetricTimeSeries.Builder builder) {
        throw new UnsupportedOperationException();
    }

    private long meanDate(BinaryTimeSeries binaryTimeSeries) {
        long start = binaryTimeSeries.getStart();
        long end = binaryTimeSeries.getEnd();

        return start + ((end - start) / 2);
    }

    @Override
    public BinaryTimeSeries to(MetricTimeSeries timeSeries) {
        throw new UnsupportedOperationException();
    }
}

