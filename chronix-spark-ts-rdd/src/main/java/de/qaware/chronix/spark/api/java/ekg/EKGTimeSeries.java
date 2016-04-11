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
package de.qaware.chronix.spark.api.java.ekg;


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;

/**
 * Created by c.hillmann on 08.04.2016.
 */
public class EKGTimeSeries implements Serializable{
    private String metric;
    private String host;
    private String process;
    private String measurement;
    private List<Pair<Long, Double>> data;


    public EKGTimeSeries(String metric, String host, String process, String measurement, List<Pair<Long, Double>> data) {
        this.metric = metric;
        this.host = host;
        this.process = process;
        this.measurement = measurement;
        this.data = data;
    }

    public String getMetric() {
        return metric;
    }

    public String getHost() {
        return host;
    }

    public String getProcess() {
        return process;
    }

    public String getMeasurement() {
        return measurement;
    }

    public List<Pair<Long, Double>> getData() { return data; }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("metric", metric)
                .append("host", host)
                .append("process", process)
                .append("measurement", measurement)
                .toString();
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
        EKGTimeSeries rhs = (EKGTimeSeries) obj;
        return new EqualsBuilder()
                .append(this.metric, rhs.metric)
                .append(this.host, rhs.host)
                .append(this.process, rhs.process)
                .append(this.measurement, rhs.measurement)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(metric)
                .append(host)
                .append(process)
                .append(measurement)
                .toHashCode();
    }
}
