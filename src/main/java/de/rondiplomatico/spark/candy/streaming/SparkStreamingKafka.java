package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.base.SparkBase;
import de.rondiplomatico.spark.candy.base.data.Crush;
import de.rondiplomatico.spark.candy.streaming.oauth.CustomAuthenticateCallbackHandler;
import de.rondiplomatico.spark.candy.streaming.oauth.SparkSQLKafkaProperties;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class SparkStreamingKafka extends SparkBase {
    private static final String KAFKA = "kafka.";

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkStreamingBasics basics = new SparkStreamingBasics();
        SparkStreamingKafka kafka = new SparkStreamingKafka();

        // TODO: Producer; send Crush Messages to Kafka
        // Filter by color

        // TODO: Consumer read messages from Kafka
        // Feedback if only one Color arrives
        // Experiment with the checkpoint location in or Aggregation functions
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

    public <T> StreamingQuery streamToKafka(Dataset<T> dataset) throws TimeoutException {
        Map<String, String> opts = getKafkaOptions();

        opts.put("checkpointLocation", "streaming-kafka");
        opts.put(SparkSQLKafkaProperties.TOPIC, "evh-sparktraining");

        return dataset.toJSON()
                      .alias("value")
                      .writeStream()
                      .format("kafka")
                      .options(opts)
                      .start();
    }

    public <T> Dataset<T> streamFromKafka(Class<T> clazz) {
        Map<String, String> opts = getKafkaOptions();

        opts.put(SparkSQLKafkaProperties.SUBSCRIBE, "evh-sparktraining");
        opts.put(SparkSQLKafkaProperties.MAX_OFFSET_PER_TRIGGER, "200");
        opts.put(SparkSQLKafkaProperties.STARTING_OFFSETS, "earliest");

        Dataset<Row> res = getSparkSession().readStream()
                                            .format("kafka")
                                            .options(opts)
                                            .load();
        res = res.select(functions.from_json(functions.col("value").cast("String"), getBeanEncoder(clazz).schema())
                                  .as("json"))
                 .select("json.*");
        return res.as(getBeanEncoder(clazz));
    }
}
