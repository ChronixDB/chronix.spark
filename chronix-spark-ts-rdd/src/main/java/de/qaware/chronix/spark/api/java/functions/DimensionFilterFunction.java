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

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.spark.api.java.function.Function;

import java.util.Map;

/**
 * Filters MetricTimeSeries according their dimensional values.
 */
public class DimensionFilterFunction implements Function<MetricTimeSeries, Boolean> {

    private Map<String, String> dimensionalFilter;
    private boolean ignoreNulls = false;

    /**
     * Default constructor.
     *
     * @param dimensionalFilter dimensions (key) and their values to filter
     */
    public DimensionFilterFunction(Map<String, String> dimensionalFilter) {
        this.dimensionalFilter = dimensionalFilter;
    }

    /**
     * Contructor including decision to ignore non-set dimensions.
     *
     * @param dimensionalFilter dimensions (key) and their values to filter
     * @param ignoreNulls       filters in time series where a dimension in the filter is not set (value = null)
     */
    public DimensionFilterFunction(Map<String, String> dimensionalFilter, boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
        this.dimensionalFilter = dimensionalFilter;
    }

    @Override
    public Boolean call(MetricTimeSeries mts) throws Exception {

        for (Map.Entry<String, String> entry : dimensionalFilter.entrySet()) {
            String value = (String) mts.attribute(entry.getKey());
            if (value == null && ignoreNulls) return true;
            else if (value == null && entry.getValue() == null) ;
            else if (value == null) return false;
            else {
                return value.equals(entry.getValue());
            }
        }
        return true; //if no filter is defined, return true
    }
}