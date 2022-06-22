/* _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2020, all rights reserved
 * _____________________________________________________________________________
 */
package de.rondiplomatico.spark.candy.streaming.oauth;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final String STARTING_OFFSETS = "startingOffsets";
    public static final String MAX_OFFSET_PER_TRIGGER = "maxOffsetsPerTrigger";
    public static final String FAIL_ON_DATA_LOSS = "failOnDataLoss";
    public static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

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

}
