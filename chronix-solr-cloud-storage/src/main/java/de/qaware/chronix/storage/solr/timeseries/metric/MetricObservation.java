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

import java.io.Serializable;

/**
 * A metric observation datatype.
 * <p>
 * Wrapper datatype to convert MetricTimeSeries to MetricObservationBeans
 * esp. useful to create Spark DataFrames.
 */
public class MetricObservation implements Serializable {

    private static final long serialVersionUID = 42L;

    private String metric;
    private String host;
    private String measurementSeries;
    private String process;
    private String ag;
    private String group;
    private long timestamp;
    private double value;

    /**
     * Empty constructor for reflection purposes / to be compliant with Java Bean specification.
     * Please use the value constructor per default.
     */
    public MetricObservation() {
    }

    /**
     * @param metric            the metric (dimension)
     * @param host              the host (dimension)
     * @param measurementSeries the measurement series (dimension)
     * @param process           the process (dimension)
     * @param group             the metric group (dimension)
     * @param ag                the aggregation level (dimension)
     * @param timestamp         the timestamp (primary dimension)
     * @param value             the value
     */
    public MetricObservation(String metric,
                             String host,
                             String measurementSeries,
                             String process,
                             String group,
                             String ag,
                             long timestamp,
                             double value) {
        this.metric = metric;
        this.host = host;
        this.measurementSeries = measurementSeries;
        this.process = process;
        this.group = group;
        this.ag = ag;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * @return the metric
     */
    public String getMetric() {
        return metric;
    }

    /**
     *
     * @param metric the metric
     */
    public void setMetric(String metric) {
        this.metric = metric;
    }

    /**
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     *
     * @param host the host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns the measurement series name.
     * <p>
     * Used "measurement" as property name to be symmetric with MetricTimeSeries attribute name
     *
     * @return measurement series name
     */
    public String getMeasurement() {
        return measurementSeries;
    }

    /**
     * Sets the measurement series name.
     * <p>
     * Used "measurement" as property name to be symmetric with MetricTimeSeries attribute name
     */
    public void setMeasurement(String measurementSeries) {
        this.measurementSeries = measurementSeries;
    }

    /**
     *
     * @return the process
     */
    public String getProcess() {
        return process;
    }

    /**
     *
     * @param process the process
     */
    public void setProcess(String process) {
        this.process = process;
    }

    /**
     *
     * @return the aggregation level
     */
    public String getAg() {
        return ag;
    }

    /**
     *
     * @param ag the aggregation level
     */
    public void setAg(String ag) {
        this.ag = ag;
    }

    /**
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     *
     * @param timestamp the timestamp
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     *
     * @return the metric group
     */
    public String getGroup() {
        return group;
    }

    /**
     *
     * @param group the metric group
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     *
     * @return the value at the timestamp
     */
    public double getValue() {
        return value;
    }

    /**
     *
     * @param value the value at the timestamp
     */
    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricObservation that = (MetricObservation) o;

        if (timestamp != that.timestamp) return false;
        if (Double.compare(that.value, value) != 0) return false;
        if (!metric.equals(that.metric)) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;
        if (measurementSeries != null ? !measurementSeries.equals(that.measurementSeries) : that.measurementSeries != null)
            return false;
        if (process != null ? !process.equals(that.process) : that.process != null) return false;
        if (ag != null ? !ag.equals(that.ag) : that.ag != null) return false;
        return group != null ? group.equals(that.group) : that.group == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = metric.hashCode();
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + (measurementSeries != null ? measurementSeries.hashCode() : 0);
        result = 31 * result + (process != null ? process.hashCode() : 0);
        result = 31 * result + (ag != null ? ag.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MetricObservation{" +
                "metric='" + metric + '\'' +
                ", host='" + host + '\'' +
                ", measurement series='" + measurementSeries + '\'' +
                ", process='" + process + '\'' +
                ", ag='" + ag + '\'' +
                ", group='" + group + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
