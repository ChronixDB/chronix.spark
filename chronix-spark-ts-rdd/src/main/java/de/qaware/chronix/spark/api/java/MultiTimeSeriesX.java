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
package de.qaware.chronix.spark.api.java;

import com.google.common.collect.Lists;
import de.qaware.chronix.timeseries.TimeSeries;

import java.util.Collections;
import java.util.List;

/**
 * Container of multiple time series.
 *
 * Contained TimeSeries are bound to datatype Long
 * for the timestamp vector and datatype Float for
 * the value vector.
 */
public class MultiTimeSeriesX {

    private List<TimeSeries<Long, Float>> ts;

    public MultiTimeSeriesX(List<TimeSeries<Long, Float>> ts){
        this.ts = Lists.newCopyOnWriteArrayList(ts);
    }

    public List<TimeSeries<Long, Float>> getTimeSeries(){
        return Lists.newCopyOnWriteArrayList(ts);
    }

    public static MultiTimeSeriesX empty() {
        return new MultiTimeSeriesX(Collections.emptyList());
    }

}
