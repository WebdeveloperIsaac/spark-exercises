/* _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2020, all rights reserved
 * _____________________________________________________________________________
 */
package de.rondiplomatico.spark.candy.streaming;

import com.daimler.ca.common.logging.CALogger;
import com.daimler.ca.properties.PropertyReader;
import com.daimler.ca.spark.flow.FlowEvent;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;

import java.util.Map;

/**
 * Contains all the configuration options for the Spark Streaming Kafka
 * integration.
 * 
 * @see <a href=
 *      "https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html">spark.apache.org/docs/latest/structured-streaming-kafka-integration</a>
 *      <p>
 *
 * @author wirtzd
 * @since 13.05.2020
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SparkSQLKafkaProperties {

    private static final CALogger log = CALogger.forClass(SparkSQLKafkaProperties.class);

    /** The Constant KAFKA. */
    private static final String KAFKA = "kafka.";

    public static final String AUTH_MODE_SASL = "SASL";
    public static final String AUTH_MODE_SASL_USER_CLIENT = "SASL_USER_CLIENT";
    public static final String AUTH_MODE_OAUTH = "OAUTH";

    public static final String AUTH_MODE = "auth.mode";

    public static final String STARTING_OFFSETS = "startingOffsets";
    public static final String MAX_OFFSET_PER_TRIGGER = "maxOffsetsPerTrigger";
    public static final String FAIL_ON_DATA_LOSS = "failOnDataLoss";
    public static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

    /** The Constant CHECKPOINT_LOCATION. */
    public static final String CHECKPOINT_LOCATION = "checkpointLocation";

    /**
     * OAuth
     */
    public static final String OAUTH_AUTHORITY = "oauth.authority";
    public static final String OAUTH_APPID = "oauth.app.id";
    public static final String OAUTH_APPSECRET = "oauth.app.secret";

    /**
     * SASL
     */
    public static final String SAS_ENDPOINT = "sasl.endpoint";
    public static final String SAS_KEY_NAME = "sasl.key.name";
    public static final String SAS_KEY = "sasl.key";
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    public static final String SECURITY_PROTOCOL = "security.protocol";
    public static final String SECURITY_PROTOCOL_SASL_SSL = "SASL_SSL";

    /**
     * For reading only. The topic list to subscribe. Only one of "assign",
     * "subscribe" or "subscribePattern" options can be specified for Kafka source.
     */
    public static final String SUBSCRIBE = "subscribe";

    /**
     * For writing only. Sets the topic that all rows will be written to in Kafka.
     * This option overrides any topic column that may exist in the data.
     */
    public static final String TOPIC = "topic";

    /**
     * Set props for Kafka, either for SASL or OAuth.
     * 
     * @param reader
     *            PropertyReader
     * @param opts
     *            the options map
     */
    public static void setAuthModeProps(PropertyReader reader, Map<String, String> opts) {
        if (SparkSQLKafkaProperties.AUTH_MODE_SASL.equals(reader.get(SparkSQLKafkaProperties.AUTH_MODE))) {
            log.log(FlowEvent.KAFKA_AUTH_MODE_SASL_WARNING);
            /*
             * Build the azure eventhubs connection string. As the only environment for use
             * is azure, there is currently no need to introduce more code structure to
             * externalize the connection string builders.
             */
            String connectionString = SASConnectionStringProvider.getSaslConnectionString(
                                                                                          reader.get(SparkSQLKafkaProperties.SAS_ENDPOINT),
                                                                                          reader.get(SparkSQLKafkaProperties.SAS_KEY_NAME),
                                                                                          reader.get(SparkSQLKafkaProperties.SAS_KEY));

            /*
             * Connection security and settings See
             * https://kafka.apache.org/0102/javadoc/org/apache/kafka/common/config/
             * SaslConfigs.html
             */
            opts.put(KAFKA + SaslConfigs.SASL_MECHANISM, "PLAIN");
            opts.put(KAFKA + SaslConfigs.SASL_JAAS_CONFIG, connectionString);

        } else if (SparkSQLKafkaProperties.AUTH_MODE_SASL_USER_CLIENT
                                                                     .equals(reader.get(SparkSQLKafkaProperties.AUTH_MODE))) {

            opts.put(KAFKA + SaslConfigs.SASL_MECHANISM, "PLAIN");
            opts.put(KAFKA + SaslConfigs.SASL_JAAS_CONFIG, reader.get(SparkSQLKafkaProperties.SASL_JAAS_CONFIG));

        } else if (SparkSQLKafkaProperties.AUTH_MODE_OAUTH.equals(reader.get(SparkSQLKafkaProperties.AUTH_MODE))) {

            opts.put("kafka.sasl.jaas.config",
                     "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            opts.put(KAFKA + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                     reader.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                                CustomAuthenticateCallbackHandler.class.getCanonicalName()));
            opts.put(KAFKA + SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

        } else {
            log.log(FlowEvent.KAFKA_AUTH_MODE_MISSING);
        }
        // Basic config
        opts.put(KAFKA + SparkSQLKafkaProperties.SECURITY_PROTOCOL, SparkSQLKafkaProperties.SECURITY_PROTOCOL_SASL_SSL);
        opts.put(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS, reader.get(SparkSQLKafkaProperties.BOOTSTRAP_SERVERS));
        opts.put(SparkSQLKafkaProperties.STARTING_OFFSETS, reader.get(SparkSQLKafkaProperties.STARTING_OFFSETS));
        opts.put(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS, reader.get(SparkSQLKafkaProperties.FAIL_ON_DATA_LOSS));
        long maxOffsetPerTrigger = reader.getLong(SparkSQLKafkaProperties.MAX_OFFSET_PER_TRIGGER, -1);
        if (maxOffsetPerTrigger != -1) {
            opts.put(SparkSQLKafkaProperties.MAX_OFFSET_PER_TRIGGER, String.valueOf(maxOffsetPerTrigger));
        }
    }
}
