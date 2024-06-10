package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.FunctionalJava;
import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.Utils;
import de.rondiplomatico.spark.candy.base.data.Candy;
import de.rondiplomatico.spark.candy.base.data.Color;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.base.data.CrushDeserializer;
import de.rondiplomatico.spark.candy.base.data.CrushSerializer;
import de.rondiplomatico.spark.candy.base.data.Deco;
import de.rondiplomatico.spark.candy.streaming.oauth.CustomAuthenticateCallbackHandler;
import de.rondiplomatico.spark.candy.streaming.oauth.SparkSQLKafkaProperties;

import org.apache.avro.data.Json;
import org.apache.commons.math3.geometry.spherical.oned.ArcsSet.Split;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Explode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryManager;
import org.eclipse.jetty.util.ajax.JSON;

import com.fasterxml.jackson.databind.JsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SparkStreamingKafka extends SparkBase {
    private static final String KAFKA = "kafka.";

    
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkStreamingBasics basics = new SparkStreamingBasics();
        SparkStreamingKafka kafka = new SparkStreamingKafka();
        
        // TODO: Producer; send Crush Messages to Kafka
        Dataset<Crush> data = basics.e2_candySource(1, 2);
        StreamingQuery query = kafka.streamToKafka(data);
//        
        query.awaitTermination(20);
        kafka.streamFromKafka(Crush.class);
        // TODO: Consumer read messages from Kafka
    }

    private Map<String, String> getKafkaOptions() {
        Map<String, String> opts = new HashMap<>();

        opts.put(KAFKA + SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
        opts.put(KAFKA + SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        opts.put(KAFKA + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CustomAuthenticateCallbackHandler.class.getCanonicalName());
        opts.put(KAFKA + SparkSQLKafkaProperties.SECURITY_PROTOCOL, SparkSQLKafkaProperties.SECURITY_PROTOCOL_SASL_SSL);
        opts.put(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS, "evhns-sparktraining.servicebus.windows.net:9093");
        opts.put(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS, "false");

        return opts;
    }

    public <T> StreamingQuery streamToKafka(Dataset<T> dataset) throws TimeoutException, StreamingQueryException {
//        This beow implementation is when you want to append data not a streaming data we can use this.

//        String bootstrapServers = "127.0.0.1:9092";
//
//        // create Producer properties
//        Properties properties = new Properties();
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CrushSerializer.class.getName());
//
//        
//        KafkaProducer<String,Crush> kfp = new KafkaProducer<>(properties);
//
//        FunctionalJava fn = new FunctionalJava();
//        
//        List<Crush> crushes = fn.e1_crush(1000);
//        List<Object> responses = new ArrayList<>();
//
//        responses = crushes.stream().map((crush) -> {
//        	ProducerRecord producer = new ProducerRecord<>("first_topic",crush);
//        	Object response = null;
//			try {
//				response = kfp.send(producer).get();
//			} catch (Exception e) {
//				e.printStackTrace();
//			} 
//        	return response;
//        }).collect(Collectors.toList());
//         kfp.flush();
//        kfp.close();
        
        StreamingQuery query =
        		dataset.toJSON().alias("value").writeStream().outputMode("append").format("kafka")
        		.option("kafka.bootstrap.servers", "localhost:9092")
        		.option("topic","first_topic")
        		.option("checkpointLocation", "C:\\hadoop-3.1.2")
        		.start();
        return query;
    }

//    public <T> Dataset<T> streamFromKafka(Class<T> clazz) {
        public <T> Dataset<Row> streamFromKafka(Class<T> clazz) throws TimeoutException, StreamingQueryException  {
        Map<String, String> opts = getKafkaOptions();
        
        Dataset<Row> res = extract();
        
        res.printSchema();
        
        StreamingQuery q1 = res.
        		writeStream().format("console").start();
        
        q1.awaitTermination();
        
        Properties props = new Properties();
        
        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "test-consumer-group";
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID );

        
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        List <String> topics = new ArrayList<>();
        topics.add("first_topic");
        
        consumer.subscribe(topics);
        
        
        ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofSeconds(10));
        List<String> result = new ArrayList<>();
        consumerRecord.forEach((record) -> {
        	result.add(record.value());
        	System.out.println("Key is : "+ record.key() + "value is" + record.value());
        });
        Dataset<Row> endResult = null;
//        JavaSparkContext sc = getJavaSparkContext();
//        JavaRDD<String> ret = sc.parallelize(result);
//        Dataset<Row> end = getSparkSession().createDataFrame(ret, String.class);
//        endResult = end.map((MapFunction<String,Crush>)row -> {
//        
//        },getBeanEncoder(clazz)).collect();
        
        return endResult;
    }
        
        public Dataset<Row> extract() {
            Dataset<Row> res = getSparkSession().readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "first_topic")
                    .load()
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
            return res;

        }
}
