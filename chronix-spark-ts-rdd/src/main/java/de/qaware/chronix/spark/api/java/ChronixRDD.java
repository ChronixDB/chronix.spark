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

import de.qaware.chronix.spark.api.java.functions.DimensionFilterFunction;
import de.qaware.chronix.storage.solr.timeseries.metric.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ChronixRDD: A time series implementation based on Chronix.
 * <p>
 * The RDD represents a set of time series - the unit of parallelization
 * is one time series. A ChronixRDD can be created by and is bound to
 * a ChronixSparkContext.
 * <p>
 * The RDD contains time series specific actions and transformations. For
 * query purposes please use ChronixSparkContext as only the Context can
 * perform predicate / aggregation pushdown right now.
 *
 * @see ChronixSparkContext
 */
public class ChronixRDD extends JavaRDD<MetricTimeSeries> {

    private static ClassTag MTS_TYPE = ClassTag$.MODULE$.apply(MetricTimeSeries.class);

    public ChronixRDD(JavaRDD<MetricTimeSeries> tsRdd) {
        super(tsRdd.rdd(), MTS_TYPE);
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
     * Action: Calculates the slope of a linear regression of every time series.
     *
     * Where: value = slope * timestamp
     * .. or:     y = slope * x
     *
     * @return the slopes (simple linear regression) of each an every time series in the RDD
     */
    public JavaDoubleRDD getSlopes() {
        return this.mapToDouble((DoubleFunction<MetricTimeSeries>) mts -> {
                    SimpleRegression regression = new SimpleRegression();
            mts.points().forEach(p -> regression.addData(p.getTimestamp(), p.getValue()));
                    return regression.getSlope();
                }
        );
    }

    /**
     * Transformation: Filters the contained time series according the given predicate.
     *
     * @param filter the predicate
     * @return the time series matching the predicate
     */
    public ChronixRDD filterTimeSeries(Function<MetricTimeSeries, Boolean> filter) {
        JavaRDD<MetricTimeSeries> mts = this.filter(filter);
        return new ChronixRDD(mts);
    }

    /**
     * Transformation: Filters MetricTimeSeries according their dimensional values.
     *
     * @param dimensionFilter dimensions (key) and their value
     * @return time series according the filter criteria
     * @see DimensionFilterFunction
     */
    public ChronixRDD filterByDimensions(Map<MetricDimension, String> dimensionFilter) {
        return this.filterTimeSeries(new DimensionFilterFunction(dimensionFilter));
    }

    /**
     * Transformation: Filters MetricTimeSeries according their dimensional values.
     *
     * @param dimensionFilter dimensions (key) and their value
     * @param ignoreNull      also include time series if dimensional value is null
     * @return time series according the filter criteria
     */
    public ChronixRDD filterByDimensions(Map<MetricDimension, String> dimensionFilter, boolean ignoreNull) {
        return this.filterTimeSeries(new DimensionFilterFunction(dimensionFilter, ignoreNull));
    }

    /**
     * Transformation: Joins the time series according their identity.
     *
     * @return joined time series
     */
    public ChronixRDD joinChunks() {
        JavaPairRDD<MetricTimeSeriesKey, Iterable<MetricTimeSeries>> groupRdd
                = this.groupBy(MetricTimeSeriesKey::new);

        JavaPairRDD<MetricTimeSeriesKey, MetricTimeSeries> joinedRdd
                = groupRdd.mapValues((Function<Iterable<MetricTimeSeries>, MetricTimeSeries>) mtsIt -> {
            MetricTimeSeriesOrdering ordering = new MetricTimeSeriesOrdering();
            List<MetricTimeSeries> orderedChunks = ordering.immutableSortedCopy(mtsIt);
            MetricTimeSeries result = null;
            for (MetricTimeSeries mts : orderedChunks) {
                if (result == null) {
                    result = new MetricTimeSeries
                            .Builder(mts.getMetric())
                            .attributes(mts.attributes()).build();
                }
                result.addAll(mts.getTimestampsAsArray(), mts.getValuesAsArray());
            }
            return result;
        });

        JavaRDD<MetricTimeSeries> resultJavaRdd =
                joinedRdd.map((Tuple2<MetricTimeSeriesKey, MetricTimeSeries> mtTuple) -> mtTuple._2);

        return new ChronixRDD(resultJavaRdd);
    }

    /**
     * Transformation: Get all values as JavaDoubleRDD.
     *
     * @return a RDD with all observation values
     */
    public JavaDoubleRDD getValuesAsRdd() {
        return this.flatMapToDouble(mts -> Arrays.asList(ArrayUtils.toObject(mts.getValuesAsArray())));
    }

    /**
     * Action: Counts the number of observations.
     *
     * @return the number of overall observations in all time series
     */
    public long countObservations() {
        JavaDoubleRDD sizesRdd = this.mapToDouble(
                (DoubleFunction<MetricTimeSeries>) value -> (double) value.size());
        return sizesRdd.sum().longValue();
    }

    /**
     * Transformation: Transforms the ChronixRDD into a RDD of MetricObservations (pair of timestamp & value + dimensions).
     *
     * @return RDD of MetricObservations
     */
    public JavaRDD<MetricObservation> toObservations() {
        return this.flatMap((FlatMapFunction<MetricTimeSeries, MetricObservation>) ts -> ts.points().map(point -> {
            //null-safe read of dimensional values
            String host = ts.attributes().get(MetricDimension.HOST) == null ? null
                    : ts.attributes().get(MetricDimension.HOST).toString();
            String series = ts.attributes().get(MetricDimension.MEASUREMENT_SERIES) == null ? null
                    : ts.attributes().get(MetricDimension.MEASUREMENT_SERIES).toString();
            String process = ts.attributes().get(MetricDimension.PROCESS) == null ? null
                    : ts.attributes().get(MetricDimension.PROCESS).toString();
            String group = ts.attributes().get(MetricDimension.METRIC_GROUP) == null ? null
                    : ts.attributes().get(MetricDimension.METRIC_GROUP).toString();
            String ag = ts.attributes().get(MetricDimension.AGGREGATION_LEVEL) == null ? null
                    : ts.attributes().get(MetricDimension.AGGREGATION_LEVEL).toString();
            //convert Point/MetricTimeSeries to MetricObservation
            return new MetricObservation(
                    ts.getMetric(),
                    host, series, process, group, ag,
                    point.getTimestamp(),
                    point.getValue()
            );
        })::iterator);
    }

    /**
     * Transformation: Derives a Spark SQL DataFrame from a ChronixRDD.
     * <p>
     * The DataFrame contains the following columns:
     * <ul>
     * <li>for each dimension (@see: de.qaware.chronix.storage.solr.timeseries.metric.MetricDimension) one column</li>
     * <li>one column for the observations' timestamp</li>
     * <li>one column for the measurement value at the observation timestamp</li>
     * </ul>
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
     * <p>
     * WARNING: The Dataset-API is currently not stable. So this is an experimental feature on Chronix Spark.
     * We've flagged it deprecated to express its experimental nature (which is semantically symmetric to legacy methods).
     * More on the Datasets API here:
     * <a href="https://databricks.com/blog/2016/01/04/introducing-spark-datasets.html">databricks Blog: Introducing Spark Datasets (4/1/2016)</a>.
     *
     * @param sqlContext an open SQLContext
     * @return a Dataset of MetricObservations
     */
    public Dataset<MetricObservation> toObservationsDataset(SQLContext sqlContext) {
        return sqlContext.createDataset(this.toObservations().rdd(), Encoders.bean(MetricObservation.class));
    }
}