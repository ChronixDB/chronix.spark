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
package de.qaware.chronix.storage.solr.timeseries.metric;

import de.qaware.chronix.timeseries.dt.DoubleList;
import de.qaware.chronix.timeseries.dt.LongList;
import de.qaware.chronix.timeseries.dt.Point;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

/**
 * A metric time series that have at least the following fields:
 * - metric name,
 * - start and end,
 * - arbitrary attributes
 * and a list of metric data points (timestamp, double value)
 *
 * @author f.lautenschlager
 */
public final class MetricTimeSeries implements Serializable {

    private static final long serialVersionUID = 5497398456431471102L;

    private String metric;
    private LongList timestamps;
    private DoubleList values;

    private MetricVector attributes = new MetricVector();

    /**
     * Private constructor.
     * To instantiate a metric time series use the builder class.
     */
    private MetricTimeSeries() {
        timestamps = new LongList(500);
        values = new DoubleList(500);
    }

    /**
     * @return a copy of the metric data points
     */
    public LongList getTimestamps() {
        return timestamps.copy();
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getTimestamps} due to no {@see LongList} initialization.
     *
     * @return a copy of the timestamps as array
     */
    public long[] getTimestampsAsArray() {
        return timestamps.toArray();
    }

    /**
     * @return a copy of the metric data points
     */
    public DoubleList getValues() {
        return values.copy();
    }

    /**
     * In some cases if one just want to access all values,
     * that method is faster than {@see getValues} due to no {@see DoubleList} initialization.
     *
     * @return a copy of the values as array
     */
    public double[] getValuesAsArray() {
        return values.toArray();
    }

    /**
     * Gets the metric data point at the index i
     *
     * @param i the index position of the metric value
     * @return the metric value
     */
    public double getValue(int i) {
        return values.get(i);
    }

    /**
     * Gets the timestamp at the given index
     *
     * @param i the index position of the time stamp
     * @return the timestamp as long
     */
    public long getTime(int i) {
        return timestamps.get(i);
    }

    /**
     * Sorts the time series values.
     */
    public void sort() {
        if (timestamps.size() > 1) {

            LongList sortedTimes = new LongList(timestamps.size());
            DoubleList sortedValues = new DoubleList(values.size());

            points().sorted((o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp())).forEachOrdered(p -> {
                sortedTimes.add(p.getTimestamp());
                sortedValues.add(p.getValue());
            });

            timestamps = sortedTimes;
            values = sortedValues;
        }
    }

    /**
     * A stream over the points
     *
     * @return the points as points
     */
    public Stream<Point> points() {
        if (timestamps.isEmpty()) {
            return Stream.empty();
        }
        return Stream.iterate(of(0), pair -> of(pair.getIndex() + 1)).limit(timestamps.size());
    }

    private Point of(int index) {
        return new Point(index, timestamps.get(index), values.get(index));
    }


    /**
     * Sets the timestamps and values as data
     *
     * @param timestamps - the timestamps
     * @param values     - the values
     */
    private void setAll(LongList timestamps, DoubleList values) {
        this.timestamps = timestamps;
        this.values = values;
    }

    /**
     * Adds all the given points to the time series
     *
     * @param timestamps the timestamps
     * @param values     the values
     */
    public final void addAll(List<Long> timestamps, List<Double> values) {
        for (int i = 0; i < timestamps.size(); i++) {
            add(timestamps.get(i), values.get(i));
        }
    }

    /**
     * @param timestamps the timestamps as long[]
     * @param values     the values as double[]
     */
    public final void addAll(long[] timestamps, double[] values) {
        this.timestamps.addAll(timestamps);
        this.values.addAll(values);
    }

    /**
     * Adds a single timestamp and value
     *
     * @param timestamp the timestamp
     * @param value     the value
     */
    public final void add(long timestamp, double value) {
        this.timestamps.add(timestamp);
        this.values.add(value);
    }

    /**
     * @return the metric name
     */
    public String getMetric() {
        return metric;
    }

    /**
     * Adds an attribute to the time series
     *
     * @param dim   the dimension
     * @param value the value
     */
    private void addAttribute(MetricDimension dim, String value) {
        attributes.add(dim, value);
    }

    /**
     * Get the attribute for the given key
     *
     * @param dim the dimension
     * @return the value as object
     */
    public String attribute(MetricDimension dim) {
        return attributes.get(dim);
    }

    /**
     * @return a copy of the attributes of this time series
     */
    public MetricVector attributes() {
        return attributes;
    }

    /**
     * Clears the time series
     */
    public void clear() {
        timestamps.clear();
        values.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        MetricTimeSeries rhs = (MetricTimeSeries) obj;
        return new EqualsBuilder()
                .append(this.getMetric(), rhs.getMetric())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getMetric())
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("metric", metric)
                .append("attributes", attributes)
                .toString();
    }


    /**
     * @return the start of the time series
     */
    public long getStart() {
        if (timestamps.isEmpty()) {
            return 0;
        } else {
            return timestamps.get(0);
        }
    }

    /**
     * @return the end of the time series
     */
    public long getEnd() {
        if (timestamps.isEmpty()) {
            return 0;
        } else {
            return timestamps.get(timestamps.size() - 1);
        }
    }

    /**
     * @return the size
     */
    public int size() {
        return timestamps.size();
    }

    /**
     * @return empty if the time series contains no points
     */
    public boolean isEmpty() {
        return timestamps.size() == 0;
    }

    /**
     * The Builder class
     */
    public static final class Builder {

        /**
         * The time series object
         */
        private MetricTimeSeries metricTimeSeries;

        /**
         * Constructs a new Builder
         *
         * @param metric the metric name
         */
        public Builder(String metric) {
            metricTimeSeries = new MetricTimeSeries();
            metricTimeSeries.metric = metric;
        }


        /**
         * @return the filled time series
         */
        public MetricTimeSeries build() {
            return metricTimeSeries;
        }


        /**
         * Sets the time series data
         *
         * @param timestamps the time stamps
         * @param values     the values
         * @return the builder
         */
        public Builder points(LongList timestamps, DoubleList values) {
            if (timestamps != null && values != null) {
                metricTimeSeries.setAll(timestamps, values);
            }
            return this;
        }

        /**
         * Adds the given single data point to the time series
         *
         * @param timestamp the timestamp of the value
         * @param value     the belonging value
         * @return the builder
         */
        public Builder point(long timestamp, double value) {
            metricTimeSeries.timestamps.add(timestamp);
            metricTimeSeries.values.add(value);
            return this;
        }

        /**
         * Adds an attribute to the class
         *
         * @param dim   the dimension
         * @param value the value of the attribute
         * @return the builder
         */
        public Builder attribute(MetricDimension dim, String value) {
            metricTimeSeries.addAttribute(dim, value);
            return this;
        }

        /**
         * Adds all attributes of a given MetricVector to this MetricVector.
         *
         * @param metricVector a MetricVector with its attributes to include
         * @return the builder
         */
        public Builder attributes(MetricVector metricVector) {
            metricTimeSeries.attributes().addAll(metricVector);
            return this;
        }

    }

}