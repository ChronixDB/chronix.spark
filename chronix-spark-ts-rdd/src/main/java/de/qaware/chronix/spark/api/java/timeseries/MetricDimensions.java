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
package de.qaware.chronix.spark.api.java.timeseries;

/**
 * Enum type containing all dimensions for
 * an metric observation and a metric time series in general.
 */
public enum MetricDimensions {
    METRIC("metric"),
    HOST("host"),
    MEASUREMENT_SERIES("measurement"),
    PROCESS("process"),
    AGGREGATION_LEVEL("ag"),
    METRIC_GROUP("group");

    private final String id;

    MetricDimensions(String id) {
        this.id = id;
    }

    public static MetricDimensions[] getIdentityDimensions() {
        return new MetricDimensions[]{
                METRIC, HOST, MEASUREMENT_SERIES, PROCESS, METRIC_GROUP, AGGREGATION_LEVEL
        };
    }

    public String getId() {
        return id;
    }

    public String toString() {
        return id;
    }
}
