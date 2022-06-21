//package de.rondiplomatico.spark.candy;
//
//import de.rondiplomatico.spark.candy.base.SparkBase;
//import de.rondiplomatico.spark.candy.base.data.Crush;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//
//public class SparkStructureStreamingBasics extends SparkBase {
//
//
//    public static void main(String[] args) {
//
//    }
//
//    public static Dataset<Crush> candySource(int rowsPerSecond, int numPartitions) {
//        return baseStreamSource(rowsPerSecond, numPartitions)
//                .flatMap((FlatMapFunction<Row, Crush>) row -> Generator.generate(row.getInt(1)).iterator(), getBeanEncoder(Crush.class));
//    }
//
//    public static Dataset<Row> baseStreamSource(int rowsPerSecond, int numPartitions) {
//        return getSparkSession().readStream().format("rate")
//                .option("rowsPerSecond", rowsPerSecond)
//                .option("numPartitions", numPartitions)
//                .load();
//    }
//}
