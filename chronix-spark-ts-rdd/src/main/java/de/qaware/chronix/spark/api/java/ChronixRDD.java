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

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
     * Action: Return iterator on contained time series.
     *
     * @return iterator on time series
     */
    public Iterator<MetricTimeSeries> iterator() {
        return this.collect().iterator();
    }


    /**
     * Action: Compute the mean value.
     *
     * @return mean value over all contained time series.
     */
    public double mean() {
        return getValuesAsRdd().mean().doubleValue();
    }

    /**
     * Action: Compute the approximate mean value.
     *
     * @return approximate mean value over all contained time series.
     */
    public double approxMean() {
        return getValuesAsRdd().meanApprox(1000).getFinalValue().mean();
    }

    /**
     * Action: Compute the min value.
     *
     * @return min value over all contained time series.
     */
    public double min() {
        return getValuesAsRdd().min().doubleValue();
    }

    /**
     * Action: Compute the max value.
     *
     * @return max value over all contained time series.
     */
    public double max() {
        return getValuesAsRdd().max().doubleValue();
    }

    /**
     * Transformation: Get all values as JavaDoubleRDD.
     *
     * @return a RDD with all observation values
     */
    public JavaDoubleRDD getValuesAsRdd() {
        return this.flatMapToDouble(
                new DoubleFlatMapFunction<MetricTimeSeries>() {
                    @Override
                    public Iterable<Double> call(MetricTimeSeries mts) throws Exception {
                        return Arrays.asList(ArrayUtils.toObject(mts.getValuesAsArray()));
                    }
                });
    }

    /**
     * Action: Counts the number of observations.
     *
     * @return the number of overall observations in all time series
     */
    public long countObservations() {
        JavaDoubleRDD sizesRdd = this.mapToDouble(
                new DoubleFunction<MetricTimeSeries>() {
                    @Override
                    public double call(MetricTimeSeries value) throws Exception {
                        return (double)value.size();
                    }
                });
        return sizesRdd.sum().longValue();
    }

    //TODO:
    // - delta()
    // - filter()
    // Transformations to other types: DataFrame
    // - easy additional functions based on DoubleRDD (esp. stats & approx)
    // - see TimeSeriesRDD
    // - see Kassiopeia TimeSeries POJO
}