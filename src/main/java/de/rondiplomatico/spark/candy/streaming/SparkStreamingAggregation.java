package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class SparkStreamingAggregation extends SparkBase {
    private static final Logger log = LoggerFactory.getLogger(SparkStreamingAggregation.class);

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkStreamingBasics basics = new SparkStreamingBasics();
        SparkStreamingAggregation aggregation = new SparkStreamingAggregation();

        Dataset<Crush> stream = basics.e2_candySource(1, 2);

        Dataset<SparkStreamingBasics.CrushWithCity> withCityDataset = basics.e4_citiesLookUp(stream);

        Dataset<Row> simpleAggregationResult = aggregation.exampleSimpleAggregation(withCityDataset, "city");
        basics.e1_streamToConsole(simpleAggregationResult).awaitTermination();


        Dataset<Result> complexAggregationResult = aggregation.exampleComplexAggregation(withCityDataset);
        basics.e1_streamToConsole(complexAggregationResult).awaitTermination();
    }


    /**
     * Exper
     * @param crushDataset
     * @param by
     * @return
     */
    public Dataset<Row> exampleSimpleAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset, String by) {
        /*
        * Experiment with watermarking and windowing see spark docu
        *
         */
        return crushDataset
                .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndWatermark>) e -> new CrushWithCityAndWatermark(new Timestamp(e.getCrush().getTime() * 1000), e.getCrush(), e.getCity()), getBeanEncoder(CrushWithCityAndWatermark.class))
                .withWatermark("ts", "30 seconds")
                .groupBy(
                        functions.window(col("ts"), "30 seconds"),
                        functions.col(by)
                ).count();
    }

    public Dataset<Result> exampleComplexAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset) {
        final ComplexAggregation func = new ComplexAggregation();
        return crushDataset
                .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndWatermark>) e -> new CrushWithCityAndWatermark(new Timestamp(e.getCrush().getTime() * 1000), e.getCrush(), e.getCity()), getBeanEncoder(CrushWithCityAndWatermark.class))
                .withWatermark("ts", "30 seconds")
                .groupByKey((MapFunction<CrushWithCityAndWatermark, String>) CrushWithCityAndWatermark::getCity, getBeanEncoder(String.class))
                .flatMapGroupsWithState(func, OutputMode.Append(),  Encoders.kryo(State.class), getBeanEncoder(Result.class), GroupStateTimeout.NoTimeout());
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
        private Timestamp windowStart;
        private Timestamp windowEnd;
        private String city;
        private int cityCount;
        private Map<String, Integer> userCount;
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
}
