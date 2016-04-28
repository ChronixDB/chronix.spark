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
package de.qaware.chronix.storage.solr.stream;

import de.qaware.chronix.converter.BinaryTimeSeries;
import de.qaware.chronix.converter.TimeSeriesConverter;
import de.qaware.chronix.timeseries.MetricTimeSeries;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;

import java.io.IOException;
import java.util.Iterator;

/**
 * Streams time series from a CloudSolrStream.
 */
public class SolrTupleStreamingService implements Iterator<MetricTimeSeries> {

    //TODO: Align with SolrStreamingService

    private CloudSolrStream solrStream;
    private TimeSeriesConverter<MetricTimeSeries> converter;

    private Tuple nextTuple;

    /**
     * @param solrStream the CloudSolrStream
     * @param converter  the converter to convert tuples into MetricTimeSeries
     */
    public SolrTupleStreamingService(CloudSolrStream solrStream, TimeSeriesConverter<MetricTimeSeries> converter) {
        this.solrStream = solrStream;
        this.converter = converter;
    }

    @Override
    public boolean hasNext() {
        Tuple tuple = readNextTuple();
        if (tuple.getString("EOF").equals("true")) {
            return false;
        } else {
            nextTuple = tuple;
            return true;
        }
    }

    @Override
    public MetricTimeSeries next() {
        if (nextTuple != null) { //tuple already read by hasNext()
            MetricTimeSeries result = convertFromTuple(nextTuple);
            nextTuple = null;
            return result;
        } else {
            return convertFromTuple(readNextTuple());
        }
    }

    /**
     * reads the next tuple in the stream.
     *
     * @return the next tuple
     */
    private Tuple readNextTuple() {
        Tuple tuple = null;
        try {
            tuple = solrStream.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tuple;
    }

    /**
     * Converts a tuple into a MetricTimeSeries
     *
     * @param tuple the tuple
     * @return a MetricTimeSeries containing the tuple data
     */
    private MetricTimeSeries convertFromTuple(Tuple tuple) {
        BinaryTimeSeries.Builder timeSeriesBuilder = new BinaryTimeSeries.Builder();
        tuple.fields.forEach((k, v) ->
                timeSeriesBuilder.field(k.toString(), v));
        MetricTimeSeries timeSeries = converter.from(timeSeriesBuilder.build(), 0l, 0l); //MetricTimeSeriesConverter does not use queryStart / queryEnd
        return timeSeries;
    }
}
