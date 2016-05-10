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
package de.qaware.chronix.spark.api.java.functions;

import de.qaware.chronix.storage.solr.timeseries.metric.MetricDimension;
import de.qaware.chronix.storage.solr.timeseries.metric.MetricTimeSeries;
import org.apache.spark.api.java.function.Function;

import java.util.Map;

/**
 * Filters MetricTimeSeries according their dimensional values.
 */
public class DimensionFilterFunction implements Function<MetricTimeSeries, Boolean> {

    private Map<MetricDimension, String> dimensionalFilter;
    private boolean ignoreNulls = false;

    /**
     * Default constructor.
     *
     * @param dimensionalFilter dimensions (key) and their values to filter
     */
    public DimensionFilterFunction(Map<MetricDimension, String> dimensionalFilter) {
        this.dimensionalFilter = dimensionalFilter;
    }

    /**
     * Contructor including decision to ignore non-set dimensions.
     *
     * @param dimensionalFilter dimensions (key) and their values to filter
     * @param ignoreNulls       filters in time series where a dimension in the filter is not set (value = null)
     */
    public DimensionFilterFunction(Map<MetricDimension, String> dimensionalFilter, boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
        this.dimensionalFilter = dimensionalFilter;
    }

    @Override
    public Boolean call(MetricTimeSeries mts) {
        for (Map.Entry<MetricDimension, String> entry : dimensionalFilter.entrySet()) {
            String value = mts.attribute(entry.getKey());
            if (value == null && !ignoreNulls) return false;
            else if (value == null && entry.getValue() != null) return false;
            else if (value == null && entry.getValue() == null) continue;
            else if (value != null && !value.equals(entry.getValue())) return false;
        }
        return true; //if no filter is defined, return true
    }
}