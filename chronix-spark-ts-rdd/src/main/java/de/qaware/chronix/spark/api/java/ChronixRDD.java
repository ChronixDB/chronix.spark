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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

/**
 * ChronixRDD: A time series implementation based on Chronix.
 *
 * The RDD represents a set of time series. The unit of parallelization
 * is one time series.
 *
 * The RDD contains time series specific actions and transformations.
 *
 * Semantics of JavaRDD<JavaRDD<JavaPairRDD<Long, Float>>>
 *   1st level: Collection of...
 *   2nd level:         time series as collection of ...
 *   3nd level:                 observations as key-value pairs with ...
 *   4th level:                             key is a timestamp ...
 *   5th level:                             value is a measurement.
 */
public class ChronixRDD extends JavaRDD<JavaRDD<JavaPairRDD<Long, Float>>> {


    /**
     *
     * @param rdd
     * @param classTag
     */
    public ChronixRDD(RDD<JavaRDD<JavaPairRDD<Long, Float>>> rdd, ClassTag<JavaRDD<JavaPairRDD<Long, Float>>> classTag) {
        super(rdd, classTag);
    }

    /**
     *
     * @return a string representation of the time series
     */
    public String toString(){
        return null;
    }

}