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

import de.qaware.chronix.spark.api.java.timeseries.MetricDimensions;
import de.qaware.chronix.spark.api.java.timeseries.MetricObservation;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import de.qaware.chronix.timeseries.dt.Point;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

/**
 * ChronixRDD: A time series implementation based on Chronix.
 *
 * The RDD represents a set of time series - the unit of parallelization
 * is one time series. A ChronixRDD can be created by and is bound to
 * a ChronixSparkContext.
 *
 * The RDD contains time series specific actions and transformations. For
 * query purposes please use ChronixSparkContext as only the Context can
 * perform predicate / aggregation pushdown right now.
 *
 * @see ChronixSparkContext
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
        return getValuesAsRdd().mean();
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
        return getValuesAsRdd().min();
    }

    /**
     * Action: Compute the max value.
     *
     * @return max value over all contained time series.
     */
    public double max() {
        return getValuesAsRdd().max();
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

    /**
     * Transformation: Transforms the ChronixRDD into a RDD of MetricObservations (pair of timestamp & value + dimensions).
     *
     * @return RDD of MetricObservations
     */
    public JavaRDD<MetricObservation> toObservations() {
        return this.flatMap(new FlatMapFunction<MetricTimeSeries, MetricObservation>(){

            @Override
            public Iterable<MetricObservation> call(MetricTimeSeries ts) throws Exception {
                return ts.points().map( new Function<Point, MetricObservation>() {
                    @Override
                    public MetricObservation apply(Point point) {
                        //null-safe read of dimensional values
                        String host = ts.attributes().get(MetricDimensions.HOST.getId()) == null ? null
                                : ts.attributes().get(MetricDimensions.HOST.getId()).toString();
                        String series = ts.attributes().get(MetricDimensions.MEASUREMENT_SERIES.getId()) == null ? null
                                : ts.attributes().get(MetricDimensions.MEASUREMENT_SERIES.getId()).toString();
                        String process = ts.attributes().get(MetricDimensions.PROCESS.getId()) == null ? null
                                : ts.attributes().get(MetricDimensions.PROCESS.getId()).toString();
                        String group = ts.attributes().get(MetricDimensions.METRIC_GROUP.getId()) == null ? null
                                : ts.attributes().get(MetricDimensions.METRIC_GROUP.getId()).toString();
                        String ag = ts.attributes().get(MetricDimensions.AGGREGATION_LEVEL.getId()) == null ? null
                                : ts.attributes().get(MetricDimensions.AGGREGATION_LEVEL.getId()).toString();
                        //convert Point/MetricTimeSeries to MetricObservation
                        return new MetricObservation(
                                ts.getMetric(),
                                host, series, process, group, ag,
                                point.getTimestamp(),
                                point.getValue()
                        );
                    }
                })::iterator;
            }
        });
    }

    /**
     * Transformation: Derives a Spark SQL DataFrame from a ChronixRDD.
     *
     * The DataFrame contains the following columns:
     * <ul>
     *   <li>for each dimension (@see: de.qaware.chronix.spark.api.java.timeseries.MetricDimensions) one column</li>
     *   <li>one column for the observations' timestamp</li>
     *   <li>one column for the measurement value at the observation timestamp</li>
     *  </ul>
     *
     * @param sqlContext an open SQLContext
     * @return a DataFrame containing the ChronixRDD data
     */
    public DataFrame toDataFrame(SQLContext sqlContext) {
        return sqlContext.createDataFrame(
                this.toObservations(),
                MetricObservation.class
        );
    }

    /**
     * Transformation: Derives a Spark Dataset of observations from a ChronixRDD.
     *
     * WARNING: The Dataset-API is currently not stable. So this is an experimental feature on Chronix Spark.
     * We've flagged it deprecated to express its experimental nature (which is semantically symmetric to legacy methods).
     * More on the Datasets API here:
     * <a href="https://databricks.com/blog/2016/01/04/introducing-spark-datasets.html">databricks Blog: Introducing Spark Datasets (4/1/2016)</a>.
     *
     * @deprecated
     * @param sqlContext an open SQLContext
     * @return a Dataset of MetricObservations
     */
    public Dataset<MetricObservation> toObservationsDataset(SQLContext sqlContext) {
        return sqlContext.createDataset(this.toObservations().rdd(), Encoders.bean(MetricObservation.class));
    }

}