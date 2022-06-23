package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SparkStreamingBasics extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamingBasics.class);


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkStreamingBasics basics = new SparkStreamingBasics();

        Dataset<RateStreamSourceRecord> rateStreamSourceRecordDataset = basics.baseStreamSource(1, 2);
        StreamingQuery q = basics.e1_streamToConsole(rateStreamSourceRecordDataset);
        q.awaitTermination();

//        Dataset<Crush> crushDataset = basics.e2_candySource(1, 2);
//        StreamingQuery q = basics.e1_streamToConsole(crushDataset);
//        q.awaitTermination();

//        Dataset<Crush> crushDataset = basics.e2_candySource(1, 2);
//        crushDataset = basics.e3_filterCrushes(crushDataset);
//        StreamingQuery q = basics.e1_streamToConsole(crushDataset);
//        q.awaitTermination();

//        Dataset<Crush> crushDataset = basics.e2_candySource(1, 2);
//        Dataset<CrushWithCity> crushWithCityDataset = basics.e4_citiesLookUp(crushDataset);
//        StreamingQuery q = basics.e1_streamToConsole(crushWithCityDataset);
//        q.awaitTermination();
    }

    /**
     * This Methods creates a streaming Dataset using a rate stream source,
     * The Dataset<Row> of the spark rate stream is encoded using a custom Pojo
     * @param rowsPerSecond data rows per second
     * @param numPartitions num partitions the data is distributed to
     */
    public Dataset<RateStreamSourceRecord> baseStreamSource(int rowsPerSecond, int numPartitions) {
        return getSparkSession()
                .readStream()
                .format("rate")
                .option("rowsPerSecond", rowsPerSecond)
                .option("numPartitions", numPartitions)
                .load()
                .map((MapFunction<Row, RateStreamSourceRecord>) e -> new RateStreamSourceRecord(e.getLong(1), e.getTimestamp(0), e.getLong(1) % 10), Encoders.bean(RateStreamSourceRecord.class));
    }

    /**
     * Writes a streaming dataset to console
     *
     * @return streaming query
     */
    public <T> StreamingQuery e1_streamToConsole(Dataset<T> dataset) throws TimeoutException {
        /*
         * Use a DataStreamWriter to start a streaming query
         * Use the format option to specify a console output
         */
        return null;
    }

    /**
     * Uses the {@link #baseStreamSource(int, int)} and transform it's output to a candy source
     */
    public Dataset<Crush> e2_candySource(int rowsPerSecond, int numPartitions) {
        /*
         * TODO:
         * Use the baseStreamSource and transform the Dataset<RateStreamSourceRecord> to a dataset<Crush>
         * Use the timestamp as crush timestamp and nCrushes as number of crushes for each RateStreamSourceRecord
         * Log the timestamp and how many candies where crushed for each RateStreamSourceRecord
         * Tip: if you need an encoder getBeanEncoder(Crush.class) is your friend
         */
        return null;
    }

    /**
     * Filters the streaming dataset of crushes by color RED
     */
    public Dataset<Crush> e3_filterCrushes(Dataset<Crush> crushDataset) {
        // TODO: Filter the crushes by color red
        return null;
    }

    /**
     * Add the city information to each crush using a broadcast, a custom Pojo containing the crush and the city is returned
     */
    public Dataset<CrushWithCity> e4_citiesLookUp(Dataset<Crush> crushDataset) {
        /*
         * TODO: Lookup the city for each crush using a broadcast
         * Use the CrushWithCity Pojo as result
         */
        return null;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class RateStreamSourceRecord implements Serializable {
        private long index;
        private Timestamp timestamp;
        private long nCrushes;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Getter
    public static class CrushWithCity implements Serializable {
        private Crush crush;
        private String city;
    }
}
