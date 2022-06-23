package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.streaming.SparkStreamingBasics.CrushWithCity;
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

        Dataset<Crush> stream = basics.e2_candySource(50, 2);

        Dataset<SparkStreamingBasics.CrushWithCity> withCityDataset = basics.e4_citiesLookUp(stream);

        Dataset<Row> simpleAggregationResult = aggregation.exampleSimpleAggregation(withCityDataset, "city");
        basics.e1_streamToConsole(simpleAggregationResult).awaitTermination();

        // Dataset<Row> windowedAggregation = aggregation.exampleWindowedAggregation(withCityDataset, "city");
        // basics.e1_streamToConsole(windowedAggregation).awaitTermination();
        //
        // Dataset<Row> windowedWithWatermarkAggregation = aggregation.exampleWindowedWithWatermarkAggregation(withCityDataset, "city");
        // basics.e1_streamToConsole(windowedWithWatermarkAggregation).awaitTermination();
        //
        // Dataset<Result> customTimeAggregation = aggregation.exampleCustomTimeAggregation(withCityDataset);
        // basics.e1_streamToConsole(customTimeAggregation).awaitTermination();
        //
        // Dataset<Result> customCrushAggregation = aggregation.exampleCustomCrushAggregation(withCityDataset);
        // basics.e1_streamToConsole(customCrushAggregation).awaitTermination();
    }

    /**
     * Simple aggregation function, aggregating the input stream by the given column. The result is contiounsly updating.
     * Therefore, only Append and Complete Mode are supported
     */
    public Dataset<Row> exampleSimpleAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset, String column) {
        return crushDataset
                           // Grouping by given column
                           .groupBy(
                                    functions.col(column))
                           .count();
    }

    /**
     * Simple windowed aggregation function, aggregating the input stream by the given column.
     * The result is windowed, even old windows might be updated due to late data.
     * Therefore, only Append and Complete Mode are supported
     */
    public Dataset<Row> exampleWindowedAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset, String column) {
        return crushDataset
                           // transformation to add Timestamp in the correct format to Dataset
                           .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndTimestamp>) SparkStreamingAggregation::wrap,
                                getBeanEncoder(CrushWithCityAndTimestamp.class))
                           // Grouping by given column and window
                           .groupBy(
                                    // // window definition
                                    functions.window(col("ts"), "30 seconds"),
                                    // grouping column definion
                                    functions.col(column))
                           .count();
    }

    /**
     * Windowed aggregation function similar to @{@link #exampleWindowedAggregation(Dataset, String)}. With one big difference.
     * Here we use watermarking to discard old data. This results in completing old aggregation windows as soon as the watermark has progressed.
     * All Output Modes can be used
     */
    public Dataset<Row> exampleWindowedWithWatermarkAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset, String column) {
        return crushDataset
                           // transformation to add Timestamp in the correct format to Dataset
                           .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndTimestamp>) SparkStreamingAggregation::wrap,
                                getBeanEncoder(CrushWithCityAndTimestamp.class))
                           // Add Watermark to Dataset; based on timestamp column and a given watermark delay
                           // Events older than the watermark = (oldest timestamps of current micro batch - delay) are discarded
                           .withWatermark("ts", "30 seconds")
                           .groupBy(
                                    // window definition
                                    functions.window(col("ts"), "30 seconds"),
                                    // grouping column definition
                                    functions.col(column))
                           .count();
    }

    /**
     * Custom Aggregation function using:
     * - a Watermark
     * - Custom Time Windowing
     *
     * The data is:
     * - grouped by city
     * - aggregated in a custom State @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.State}
     * - evaluated by a custom time condition defined in @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.CustomTimeAggregation}
     *
     * A custom Dataset of a custom result class is returned: @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.Result}
     */
    public Dataset<Result> exampleCustomTimeAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset) {
        final CustomTimeAggregation func = new CustomTimeAggregation();
        return crushDataset
                           // transformation to add Timestamp in the correct format to Dataset
                           .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndTimestamp>) SparkStreamingAggregation::wrap,
                                getBeanEncoder(CrushWithCityAndTimestamp.class))
                           // Add Watermark to Dataset; based on timestamp column and a given watermark delay
                           // Events older than the watermark = (oldest timestamps of current micro batch - delay) are discarded
                           .withWatermark("ts", "30 seconds")
                           // group by key to create KeyValueGroupedDataset
                           .groupByKey((MapFunction<CrushWithCityAndTimestamp, String>) CrushWithCityAndTimestamp::getCity, getBeanEncoder(String.class))
                           // Execute custom Logic with State on KeyValueGroupedDataset
                           // The Custom Aggregation function the Append Mode and the Encoders for Custom State and Result class need to be specified
                           .flatMapGroupsWithState(func, OutputMode.Append(), Encoders.kryo(State.class), getBeanEncoder(Result.class),
                                                   GroupStateTimeout.NoTimeout());
    }

    /**
     * Custom Aggregation function using:
     * - a Watermark
     * - Custom Record based windowing: An output is generated every 1000 records by each city
     *
     * The data is:
     * - grouped by city
     * - aggregated in a custom State @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.State}
     * - evaluated by a custom condition defined in @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.CustomCrushBasedAggregation}
     *
     * A custom Dataset of a custom result class is returned: @{@link de.rondiplomatico.spark.candy.streaming.SparkStreamingAggregation.Result}
     */
    public Dataset<Result> exampleCustomCrushAggregation(Dataset<SparkStreamingBasics.CrushWithCity> crushDataset) {
        final CustomCrushBasedAggregation func = new CustomCrushBasedAggregation();
        return crushDataset
                           // transformation to add Timestamp in the correct format to Dataset
                           .map((MapFunction<SparkStreamingBasics.CrushWithCity, CrushWithCityAndTimestamp>) SparkStreamingAggregation::wrap,
                                getBeanEncoder(CrushWithCityAndTimestamp.class))
                           // Add Watermark to Dataset; based on timestamp column and a given watermark delay
                           // Events older than the watermark = (oldest timestamps of current micro batch - delay) are discarded
                           .withWatermark("ts", "30 seconds")
                           // group by key to create KeyValueGroupedDataset
                           .groupByKey((MapFunction<CrushWithCityAndTimestamp, String>) e -> e.getCrush().getCandy().getColor().toString(),
                                       getBeanEncoder(String.class))
                           // Execute custom Logic with State on KeyValueGroupedDataset
                           // The Custom Aggregation function the Append Mode and the Encoders for Custom State and Result class need to be specified
                           .flatMapGroupsWithState(func, OutputMode.Append(), Encoders.kryo(State.class), getBeanEncoder(Result.class),
                                                   GroupStateTimeout.NoTimeout());
    }

    public static class CustomTimeAggregation implements FlatMapGroupsWithStateFunction<String, CrushWithCityAndTimestamp, State, Result> {

        @Override
        public Iterator<Result> call(String key, Iterator<CrushWithCityAndTimestamp> values, GroupState<State> state) {

            log.info("Working on key: {}, time: {}, state: {}", key, state.getCurrentWatermarkMs(), state);

            List<Result> res = new ArrayList<>();
            State s;
            if (!state.exists()) {
                log.info("Init State for key: {}, state: {}", key, state);
                // State does not exist for the current key; So we need to create one :)
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
            }
            s = state.get();
            if (s.getTime() + (30 * 1000) < state.getCurrentWatermarkMs()) {
                // Custom evaluation condition is true so we generate a Result
                log.info("Evaluating State for key: {}, state: {}", key, state);
                res.add(new Result(new Timestamp(s.getTime()), new Timestamp(state.getCurrentWatermarkMs()), s.getCity(),
                                   s.getUsersCounter().values().stream().mapToInt(i -> i).sum(), new HashMap<>(s.getUsersCounter())));
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
                s = state.get();
            }
            log.info("Adding values to State for key: {}, state: {}", key, state);
            // Always aggregate the values of our current batch to the state, theoretically we could only add values if another custom condition is fulfilled.
            s.add(values);

            return res.iterator();
        }
    }

    public static class CustomCrushBasedAggregation implements FlatMapGroupsWithStateFunction<String, CrushWithCityAndTimestamp, State, Result> {

        @Override
        public Iterator<Result> call(String key, Iterator<CrushWithCityAndTimestamp> values, GroupState<State> state) {

            log.info("Working on key: {}, time: {}, state: {}", key, state.getCurrentWatermarkMs(), state);

            List<Result> res = new ArrayList<>();
            State s;
            if (!state.exists()) {
                log.info("Init State for key: {}, state: {}", key, state);
                // State does not exist for the current key; So we need to create one :)
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
            }
            s = state.get();
            if (s.getUsersCounter().values().stream().mapToInt(i -> i).sum() > 1000) {
                // Custom evaluation condition is true so we generate a Result
                log.info("Evaluating State for key: {}, state: {}", key, state);
                res.add(new Result(new Timestamp(s.getTime()), new Timestamp(state.getCurrentWatermarkMs()), s.getCity(),
                                   s.getUsersCounter().values().stream().mapToInt(i -> i).sum(), new HashMap<>(s.getUsersCounter())));
                state.update(new State(state.getCurrentWatermarkMs(), key, new HashMap<>()));
                s = state.get();
            }
            log.info("Adding values to State for key: {}, state: {}", key, state);
            // Always aggregate the values of our current batch to the state, theoretically we could only add values if another custom condition is fulfilled.
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

        public void add(Iterator<CrushWithCityAndTimestamp> v) {
            v.forEachRemaining(e -> {
                int counter = usersCounter.getOrDefault(e.getCrush().getUser(), 0);
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
        private String key;
        private int keyCount;
        private Map<String, Integer> userCount;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Getter
    public static class CrushWithCityAndTimestamp implements Serializable {
        private Timestamp ts;
        private Crush crush;
        private String city;
    }

    private static CrushWithCityAndTimestamp wrap(CrushWithCity c) {
        return new CrushWithCityAndTimestamp(new Timestamp(c.getCrush()
                                                            .getTime()
                                                            .toNanoOfDay()
                        * 1000), c.getCrush(), c.getCity());
    }
}
