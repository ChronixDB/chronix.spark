/**
 * _____________________________________________________________________________
 * Project   : Chronix Spark
 * License   : Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
 * Designed and handcrafted in Germany, (c) 2016 QAware GmbH
 * _____________________________________________________________________________
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
 * TimeseriesRDD implementation based on Chronix.
 */
public class TimeseriesRDD extends RDD<Float> {
    private static final long serialVersionUID = 1L;

    private static final ClassTag<Float> FLOAT_TAG = ClassManifestFactory$.MODULE$.fromClass(Float.class);

    public TimeseriesRDD(SparkContext sc) {
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
}
