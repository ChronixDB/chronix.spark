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

import com.google.common.collect.Ordering;
import de.qaware.chronix.timeseries.MetricTimeSeries;

import java.io.Serializable;

/**
 * Orders MetricTimeSeries by their start timestamp
 * <p>
 * left < right -> -1
 * left = right = 0
 * left > right -> +1
 */
public class MetricTimeSeriesOrdering extends Ordering<MetricTimeSeries> implements Serializable {

    private static final long serialVersionUID = 42L;

    @Override
    public int compare(MetricTimeSeries left, MetricTimeSeries right) {
        if (left == null && right == null) {
            return 0;
        } else if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else if (left.getStart() < right.getStart()) {
            return -1;
        } else if (left.getStart() == right.getStart()) {
            return 0;
        } else {
            return 1;
        }
    }
}