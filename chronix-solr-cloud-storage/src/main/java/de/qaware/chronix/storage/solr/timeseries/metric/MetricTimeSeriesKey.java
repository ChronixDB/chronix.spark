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

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * Represents the identity of a MetricTimeSeries.
 * The identity is expressed as
 */
public class MetricTimeSeriesKey implements Serializable {

    private static final long serialVersionUID = 42L;

    private MetricTimeSeries mts;

    /**
     * Default constructor
     *
     * @param mts
     */
    public MetricTimeSeriesKey(MetricTimeSeries mts) {
        this.mts = mts;
    }

    private MetricTimeSeries getMetricTimeSeries() {
        return mts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricTimeSeriesKey that = (MetricTimeSeriesKey) o;

        EqualsBuilder eb = new EqualsBuilder();
        for (MetricDimensions dim : MetricDimensions.getIdentityDimensions()) {
            if (dim == MetricDimensions.METRIC) {
                eb.append(mts.getMetric(), that.getMetricTimeSeries().getMetric());
            } else {
                eb.append(mts.attributes().get(dim.getId()),
                        that.getMetricTimeSeries().attributes().get(dim.getId()));
            }
        }
        return eb.isEquals();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hb = new HashCodeBuilder(17, 37);

        for (MetricDimensions dim : MetricDimensions.getIdentityDimensions()) {
            if (dim == MetricDimensions.METRIC) {
                hb.append(mts.getMetric());
            } else {
                hb.append(mts.attributes().get(dim.getId()));
            }
        }

        return hb.toHashCode();
    }
}
