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
package de.qaware.chronix.spark.api.java.spark.api.java;

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassTag$;

import java.util.Iterator;

/**
 * ChronixRDD: A time series implementation based on Chronix.
 *
 * The RDD represents a set of time series. The unit of parallelization
 * is one time series.
 *
 * The RDD contains time series specific actions and transformations.
 *
 */
public class ChronixRDD extends JavaRDD<MetricTimeSeries> {

    public ChronixRDD(JavaRDD<MetricTimeSeries> tsRdd) {
       super(tsRdd.rdd(), ClassTag$.MODULE$.apply(MetricTimeSeries.class));
    }

    /**
     *
     * @return iterator on time series
     */
    public Iterator<MetricTimeSeries> iterator() {
        return this.collect().iterator();
    }

}