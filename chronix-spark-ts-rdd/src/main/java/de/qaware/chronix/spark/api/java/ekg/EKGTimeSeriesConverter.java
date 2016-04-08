package de.qaware.chronix.spark.api.java.ekg;

import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by c.hillmann on 08.04.2016.
 */
public final class EKGTimeSeriesConverter {

    public static EKGTimeSeries fromMetricTimeSeries(MetricTimeSeries t) {
        List<Pair<Long, Double>> data = new ArrayList<>();

        // to slow for ekg production data !
                for (int i = 0; i < t.size(); i++) {
                    data.add(new ImmutablePair<Long, Double>(t.getTimestamps().get(i), t.getValues().get(i)));
                }

        // todo add missing fields
        EKGTimeSeries result = new EKGTimeSeries(
                t.getMetric(),
                (String) t.attribute("host"),
                (String) t.attribute("process"),
                (String) t.attribute("measurement"),
                data);
        return result;


    }
}
