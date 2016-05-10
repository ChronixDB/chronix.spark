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

import de.qaware.chronix.converter.common.Compression;
import de.qaware.chronix.converter.serializer.gen.SimpleProtocolBuffers;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries;
import de.qaware.chronix.timeseries.dt.DoubleList;
import de.qaware.chronix.timeseries.dt.LongList;
import de.qaware.chronix.timeseries.dt.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Class to easily convert the protocol buffer into Point<Long,Double>
 *
 * @author f.lautenschlager
 */
public final class ProtoBufMetricTimeSeriesSerializer {

    /**
     * Name of the system property to set the equals offset between the dates.
     */
    public static final String DATE_EQUALS_OFFSET_MS = "DATE_EQUALS_OFFSET_MS";

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufMetricTimeSeriesSerializer.class);
    private static final long ALMOST_EQUALS_OFFSET_MS = Long.parseLong(System.getProperty(DATE_EQUALS_OFFSET_MS, "10"));

    /**
     * Private constructor
     */
    private ProtoBufMetricTimeSeriesSerializer() {
        //utility class
    }

    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, MetricTimeSeries.Builder builder) {
        from(compressedBytes, timeSeriesStart, timeSeriesEnd, timeSeriesStart, timeSeriesEnd, builder);
    }


    /**
     * Adds the points (compressed byte array) to the given builder
     *
     * @param compressedBytes the compressed bytes holding the data points
     * @param timeSeriesStart the start of the time series
     * @param timeSeriesEnd   the end of the time series
     * @param from            including points from
     * @param to              including points to
     * @param builder         the time series builder
     */
    public static void from(final byte[] compressedBytes, long timeSeriesStart, long timeSeriesEnd, long from, long to, MetricTimeSeries.Builder builder) {
        if (from == -1 || to == -1) {
            throw new IllegalArgumentException("FROM or TO have to be >= 0");
        }

        //if to is left of the time series, we have no points to return
        if (to < timeSeriesStart) {
            return;
        }
        //if from is greater  to, we have nothing to return
        if (from > to) {
            return;
        }

        //if from is right of the time series we have nothing to return
        if (from > timeSeriesEnd) {
            return;
        }

        try {
            InputStream decompressedPointStream = Compression.decompressToStream(compressedBytes);
            SimpleProtocolBuffers.Points protocolBufferPoints = SimpleProtocolBuffers.Points.parseFrom(decompressedPointStream);

            long lastOffset = ALMOST_EQUALS_OFFSET_MS;
            long calculatedPointDate = timeSeriesStart;

            List<SimpleProtocolBuffers.Point> pList = protocolBufferPoints.getPList();

            int size = pList.size();
            SimpleProtocolBuffers.Point[] points = pList.toArray(new SimpleProtocolBuffers.Point[0]);

            long[] timestamps = new long[pList.size()];
            double[] values = new double[pList.size()];

            int lastPointIndex = 0;

            for (int i = 0; i < size; i++) {
                SimpleProtocolBuffers.Point p = points[i];

                if (i > 0) {
                    long offset = p.getT();
                    if (offset != 0) {
                        lastOffset = offset;
                    }
                    calculatedPointDate += lastOffset;
                }

                //only add the point if it is within the date
                if (calculatedPointDate >= from && calculatedPointDate <= to) {
                    timestamps[i] = calculatedPointDate;
                    values[i] = p.getV();
                    lastPointIndex++;
                }
            }
            builder.points(new LongList(timestamps, lastPointIndex), new DoubleList(values, lastPointIndex));

        } catch (IOException e) {
            LOGGER.info("Could not decode protocol buffers points");
        }

    }

    /**
     * Converts the given iterator of our point class to protocol buffers and compresses (gzip) it.
     *
     * @param metricDataPoints - the list with points
     * @return a protocol buffer points object
     */
    public static byte[] to(Iterator<Point> metricDataPoints) {
        long previousDate = 0;
        long previousOffset = 0;

        int timesSinceLastOffset = 1;
        long lastStoredDate = 0;

        SimpleProtocolBuffers.Point.Builder builder = SimpleProtocolBuffers.Point.newBuilder();
        SimpleProtocolBuffers.Points.Builder points = SimpleProtocolBuffers.Points.newBuilder();


        while (metricDataPoints.hasNext()) {

            Point p = metricDataPoints.next();

            if (p == null) {
                LOGGER.debug("Skipping 'null' point.");
                continue;
            }

            long offset;
            if (previousDate == 0) {
                offset = 0;
            } else {
                offset = p.getTimestamp() - previousDate;
            }

            //Semantic Compression
            if (ALMOST_EQUALS_OFFSET_MS == -1) {
                builder.clearT()
                        .setV(p.getValue());
                points.addP(builder.build());
            } else {
                if (almostEquals(previousOffset, offset) && noDrift(p.getTimestamp(), lastStoredDate, timesSinceLastOffset)) {
                    builder.clearT()
                            .setV(p.getValue());
                    points.addP(builder.build());
                    timesSinceLastOffset += 1;

                } else {
                    builder.setT(offset)
                            .setV(p.getValue())
                            .build();
                    points.addP(builder.build());
                    //reset the offset counter
                    timesSinceLastOffset = 1;
                    lastStoredDate = p.getTimestamp();
                }
                //set current as former previous date
                previousOffset = offset;
                previousDate = p.getTimestamp();
            }
        }

        return Compression.compress(points.build().toByteArray());
    }

    private static boolean noDrift(long timestamp, long lastStoredDate, int timesSinceLastOffset) {
        long calculatedMaxOffset = ALMOST_EQUALS_OFFSET_MS * timesSinceLastOffset;
        long drift = lastStoredDate + calculatedMaxOffset - timestamp;

        return (drift <= (ALMOST_EQUALS_OFFSET_MS / 2));
    }

    private static boolean almostEquals(long previousOffset, long offset) {
        double diff = Math.abs(offset - previousOffset);
        return (diff <= ALMOST_EQUALS_OFFSET_MS);
    }
}

