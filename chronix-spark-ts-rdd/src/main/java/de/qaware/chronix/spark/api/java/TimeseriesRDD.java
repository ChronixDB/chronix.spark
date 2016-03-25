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

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

/**
 * TimeSeriesRDD implementation based on Chronix.
 *
 * TODO: JavaPairRDD<JavaRDD<Long>, JavaRDD<Float>>
 */
public class TimeSeriesRDD extends RDD<Float> {
    private static final long serialVersionUID = 1L;

    private static final ClassTag<Float> FLOAT_TAG = ClassManifestFactory$.MODULE$.fromClass(Float.class);

    public TimeSeriesRDD(SparkContext sc) {
        super(sc, new ArrayBuffer<Dependency<?>>(), FLOAT_TAG);
    }

    @Override
    public Iterator<Float> compute(Partition split, TaskContext context) {
        return null;
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[0];
    }

    /**
     * Action to get all time series as plain java datatype.
     *
     * @return in-memory representation of all contained time series
     */
    public MultiTimeSeries toTimeSeries() {
        return MultiTimeSeries.empty();
    }
}