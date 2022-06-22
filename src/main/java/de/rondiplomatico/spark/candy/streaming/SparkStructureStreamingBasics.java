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
import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class SparkStructureStreamingBasics extends SparkBase {

    private static final Logger log = LoggerFactory.getLogger(SparkStructureStreamingBasics.class);


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkStructureStreamingBasics basics = new SparkStructureStreamingBasics();

        Dataset<Crush> stream = basics.e2_candySource(1, 2);
//        stream = basics.filterCrushes(stream);

//        DataStreamWriter<?> writer = basics.e1_streamToConsole(stream);
//        DataStreamWriter<?> writer = basics.e1_streamToConsole(basics.e4_citiesLookUp(stream));
//        DataStreamWriter<?> writer = basics.e1_streamToConsole(basics.e5_aggregateCrushes(basics.e4_citiesLookUp(stream), "city"));
        basics.e1_streamToConsole(basics.e6_complexAggregation(basics.e4_citiesLookUp(stream))).start().awaitTermination();
//        writer.start().awaitTermination();
    }

    public Dataset<Crush> e2_candySource(int rowsPerSecond, int numPartitions) {
        return baseStreamSource(rowsPerSecond, numPartitions)
                .flatMap(
                        (FlatMapFunction<RateStreamSourceRecord, Crush>) e -> {
                            int nCrushes = (int) e.getNCrushes();
                            List<Crush> crushList = new ArrayList<>(nCrushes);
                            for (int i = 0; i < nCrushes; i++) {
                                crushList.add(new Crush(new Candy(Utils.randColor(), Utils.randDeco()), Utils.randUser(), e.getTimestamp().getTime() / 1000));
                            }
                            return crushList.iterator();
                        }, getBeanEncoder(Crush.class));
    }

    public Dataset<RateStreamSourceRecord> baseStreamSource(int rowsPerSecond, int numPartitions) {
        return getSparkSession()
                .readStream()
                .format("rate")
                .option("rowsPerSecond", rowsPerSecond)
                .option("numPartitions", numPartitions)
                .load()
                .map((MapFunction<Row, RateStreamSourceRecord>) e -> new RateStreamSourceRecord(e.getLong(1), e.getTimestamp(0), e.getLong(1) % 10), Encoders.bean(RateStreamSourceRecord.class));
    }

    public <T> DataStreamWriter<T> e1_streamToConsole(Dataset<T> dataset) {
        return dataset.writeStream()
                .format("console")
                .option("truncate", false)
                .option("checkpointLocation", "streaming/checkpoint/" + System.currentTimeMillis())
                .outputMode(OutputMode.Append())
                ;
    }

    public Dataset<Crush> e3_filterCrushes(Dataset<Crush> crushDataset) {
        return crushDataset.filter((FilterFunction<Crush>) e -> Color.RED.equals(e.getCandy().getColor()));
    }

    public Dataset<CrushWithCity> e4_citiesLookUp(Dataset<Crush> crushDataset) {

        Map<String, String> homeMap = Utils.getHomeCities();
        final Broadcast<Map<String, String>> homeBC = getJavaSparkContext().broadcast(new HashMap<>(homeMap));

        return crushDataset.map((MapFunction<Crush, CrushWithCity>) e -> new CrushWithCity(e, homeBC.getValue().get(e.getUser())), getBeanEncoder(CrushWithCity.class));
    }

    public Dataset<Row> e5_aggregateCrushes(Dataset<CrushWithCity> crushDataset, String by) {
        return crushDataset
                .toDF().withColumn("ts", functions.to_timestamp(col("crush.time")))
                .withWatermark("ts", "30 seconds")
                .groupBy(
                        functions.window(col("ts"), "30 seconds"),
                        col(by)
                ).count();
    }

    public Dataset<Result> e6_complexAggregation(Dataset<CrushWithCity> crushDataset) {
        final ComplexAggregation func = new ComplexAggregation();
        return crushDataset
//                .toDF().withColumn("ts", functions.to_timestamp(col("crush.time")))
                .map((MapFunction<CrushWithCity, CrushWithCityAndWatermark>) e -> new CrushWithCityAndWatermark(new Timestamp(e.getCrush().getTime() * 1000), e.getCrush(), e.getCity()), getBeanEncoder(CrushWithCityAndWatermark.class))
                .withWatermark("ts", "30 seconds")
                .groupByKey((MapFunction<CrushWithCityAndWatermark, String>) CrushWithCityAndWatermark::getCity, getBeanEncoder(String.class))
                .flatMapGroupsWithState(func, OutputMode.Append(),  Encoders.kryo(State.class), getBeanEncoder(Result.class), GroupStateTimeout.NoTimeout());
//                ).count();
    }

    public static class ComplexAggregation implements FlatMapGroupsWithStateFunction<String, CrushWithCityAndWatermark, State, Result> {

        @Override
        public Iterator<Result> call(String key, Iterator<CrushWithCityAndWatermark> values, GroupState<State> state) {

            log.info("Working on key: {}, time: {}, state: {}", key, state.getCurrentWatermarkMs(), state);

            List<Result> res = new ArrayList<>();
            State s;
            if (!state.exists()) {
                log.info("Init State for key: {}, state: {}", key, state);
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
            }
            s = state.get();
            if (s.getTime() + (30 * 1000) < state.getCurrentWatermarkMs() ) {
                log.info("Evaluating State for key: {}, state: {}", key, state);
                res.add(new Result(new Timestamp(s.getTime()), new Timestamp(state.getCurrentWatermarkMs()), s.getCity(), s.getUsersCounter().values().stream().mapToInt(i -> i).sum(), new HashMap<>(s.getUsersCounter())));
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
                s = state.get();
            }
            log.info("Adding values to State for key: {}, state: {}", key, state);
            s.add(values);

            return res.iterator();
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class State implements Serializable {
        private long time;
        private String city;
        private Map<String, Integer> usersCounter;

        public void add(Iterator<CrushWithCityAndWatermark> v) {
            v.forEachRemaining(e -> {
                int counter = usersCounter.getOrDefault(e.getCrush().getUser(),0);
                usersCounter.put(e.getCrush().getUser(), counter + 1);
            });
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class Result implements Serializable {
        Timestamp windowStart;
        Timestamp windowEnd;
        String city;
        int cityCount;
        Map<String, Integer> userCount;
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Getter
    public static class CrushWithCityAndWatermark implements Serializable {
        private Timestamp ts;
        private Crush crush;
        private String city;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Getter
    public static class CrushWithCity implements Serializable {
        private Crush crush;
        private String city;
    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class RateStreamSourceRecord implements Serializable {
        private long index;
        private Timestamp timestamp;
        private long nCrushes;
    }
}
