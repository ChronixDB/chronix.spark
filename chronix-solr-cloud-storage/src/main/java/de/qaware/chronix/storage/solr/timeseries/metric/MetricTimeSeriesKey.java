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
import java.util.Collections;
import java.util.Map;

/**
 * Represents the identity of a MetricTimeSeries.
 * The identity is expressed as
 */
public class MetricTimeSeriesKey implements Serializable {

    private static final long serialVersionUID = 42L;

    private final String metric;
    private final Map<String, Object> attributes;

    public MetricTimeSeriesKey(MetricTimeSeries mts) {
        this.metric = mts.getMetric();
        this.attributes = mts.attributes();
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricTimeSeriesKey that = (MetricTimeSeriesKey) o;

        EqualsBuilder eb = new EqualsBuilder();
        for (MetricDimensions dim : MetricDimensions.getIdentityDimensions()) {
            if (dim == MetricDimensions.METRIC) {
                eb.append(this.metric, that.metric);
            } else {
                eb.append(this.attributes.get(dim.getId()), that.attributes.get(dim.getId()));
            }
        }
        return eb.isEquals();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hb = new HashCodeBuilder(17, 37);

        for (MetricDimensions dim : MetricDimensions.getIdentityDimensions()) {
            if (dim == MetricDimensions.METRIC) {
                hb.append(this.metric);
            } else {
                hb.append(this.attributes.get(dim.getId()));
            }
        }

        return hb.toHashCode();
    }
}
