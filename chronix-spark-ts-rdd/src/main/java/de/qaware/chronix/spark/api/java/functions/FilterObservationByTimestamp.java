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

import de.qaware.chronix.spark.api.java.timeseries.MetricObservation;
import org.apache.spark.api.java.function.FilterFunction;

/**
 * Filters out observations at a given timestamp
 * (or at a time range around the timestamp).
 */
public class FilterObservationByTimestamp implements FilterFunction<MetricObservation> {

    private double timestamp;
    private double range = 0.0d;

    /**
     * Value constructor.
     *
     * @param timestamp the timestamp to use to filter
     * @param range     a time range around the timestamp where:
     *                  timestamp - range <= observation.timestamp <= timestamp + range
     */
    public FilterObservationByTimestamp(double timestamp, double range) {
        this.timestamp = timestamp;
        this.range = range;
    }

    /**
     * Value constructor.
     * Range is set to 0.0d
     *
     * @param timestamp the timestamp to use to filter
     */
    public FilterObservationByTimestamp(double timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean call(MetricObservation value) throws Exception {
        return value.getTimestamp() >= timestamp - range && value.getTimestamp() <= timestamp + range;
    }
}
