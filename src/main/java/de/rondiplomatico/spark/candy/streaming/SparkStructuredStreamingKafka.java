package de.rondiplomatico.spark.candy.streaming;

import de.rondiplomatico.spark.candy.base.SparkBase;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.HashMap;
import java.util.Map;

public class SparkStructuredStreamingKafka extends SparkBase {

    public <T> StreamingQuery streamToKafka(Dataset<T> dataset) {
        Map<String, String> opts = new HashMap<>();


        opts.put("kafka." + SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        opts.put("kafka." + SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        opts.put("kafka." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, propertyReader.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CustomAuthenticateCallbackHandler.class.getCanonicalName()));
        opts.put("checkpointLocation", propertyReader.get(SparkSQLKafkaProperties.CHECKPOINT_LOCATION));
        opts.put("kafka." + "security.protocol", "SASL_SSL");
        opts.put(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS, propertyReader.get(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS));
        opts.put(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS, propertyReader.get(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS));
        opts.put(SparkSQLKafkaProperties.TOPIC, propertyReader.get(SparkSQLKafkaProperties.TOPIC));

        StreamingQuery q = dataset.toJSON()
                .alias("value")
                .writeStream()
                .format("kafka")
                .options(opts)
                .start();
    }

    public <T> Dataset<T> streamFromKafka(Class<T> clazz) {
        Map<String, String> opts = new HashMap<>();

        opts.put(SparkSQLKafkaProperties.SUBSCRIBE, reader.get(SparkSQLKafkaProperties.SUBSCRIBE));


        opts.put("kafka." + SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        opts.put("kafka." + SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        opts.put("kafka." + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, propertyReader.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, CustomAuthenticateCallbackHandler.class.getCanonicalName()));
        opts.put("checkpointLocation", propertyReader.get(SparkSQLKafkaProperties.CHECKPOINT_LOCATION));
        opts.put("kafka." + "security.protocol", "SASL_SSL");
        opts.put(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS, propertyReader.get(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS));
        opts.put(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS, propertyReader.get(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS));
        opts.put(SparkSQLKafkaProperties.TOPIC, propertyReader.get(SparkSQLKafkaProperties.TOPIC));

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
