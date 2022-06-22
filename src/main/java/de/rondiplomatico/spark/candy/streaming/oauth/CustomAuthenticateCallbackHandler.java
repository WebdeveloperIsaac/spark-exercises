package de.rondiplomatico.spark.candy.streaming.oauth;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *
 * The configured callback handler is responsible for validating the password provided by clients and this may use an external authentication server.
 *
 * Code differs from the tutorial to avoid double checked locking in getOAuthBearerToken method by using volatile.
 *
 * @see <a href=
 *      "https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/tutorials/oauth/java/appsecret/producer/src/main/java">azure-event-hubs-for-kafka
 *      tutorial</a>
 *
 * @author schmidts
 * @since 21.12.2020
 */
public class CustomAuthenticateCallbackHandler implements AuthenticateCallbackHandler {

    private static final Logger logger = LoggerFactory.getLogger(CustomAuthenticateCallbackHandler.class);

    public static final String SPARK_OAUTH_APP_ID = "spark.oauth.app.id";
    public static final String SPARK_OAUTH_APP_SECRET = "spark.oauth.app.secret";
    public static final String SPARK_OAUTH_AUTHORITY = "spark.oauth.authority";
    public static final String SPARK_CONF = "spark.conf";

    private String authority;
    private String appId;
    private String appSecret;
    private volatile ConfidentialClientApplication aadClient;
    private ClientCredentialParameters aadParameters;

    /**
     * Configure callback for bearer token.
     * 
     * @param configs
     *            requires configuraton bootstrap.servers
     * @param mechanism
     *            not used
     * @param jaasConfigEntries
     *            requires config of sasl.jaas.config
     */
    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        String bootstrapServer = Arrays.asList(configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).get(0).toString();
        bootstrapServer = bootstrapServer.replaceAll("\\[|\\]", "");
        URI uri = URI.create("https://" + bootstrapServer);
        String sbUri = uri.getScheme() + "://" + uri.getHost();
        this.aadParameters =
                        ClientCredentialParameters.builder(Collections.singleton(sbUri + "/.default"))
                                                  .build();
        if (!SparkSession.getActiveSession().isEmpty()) {
            SparkSession ss = SparkSession.getActiveSession().get();
            logger.info("Loading OAuth credentials from {}", "active session " + ss.sparkContext().appName());
            this.appId = ss.conf().get(SPARK_OAUTH_APP_ID);
            this.appSecret = ss.conf().get(SPARK_OAUTH_APP_SECRET);
            this.authority = ss.conf().get(SPARK_OAUTH_AUTHORITY);
        } else if (Files.exists(Paths.get(SparkFiles.get(SPARK_CONF)))) {
            logger.info("Loading OAuth credentials from {}", SPARK_CONF);
            try (FileReader fr = new FileReader(SparkFiles.get(SPARK_CONF))) {
                Properties p = new Properties();
                p.load(fr);
                this.appId = p.getProperty(SPARK_OAUTH_APP_ID);
                this.appSecret = p.getProperty(SPARK_OAUTH_APP_SECRET);
                this.authority = p.getProperty(SPARK_OAUTH_AUTHORITY);
            } catch (IOException e) {
                logger.error("spark.conf could not be read.", e);
            }
        } else {
            SparkContext sc = SparkContext.getOrCreate();
            if (sc.isLocal()) {
                logger.info("Loading OAuth credentials from {}", "spark context " + sc.appName());
                this.appId = sc.conf().get(SPARK_OAUTH_APP_ID);
                this.appSecret = sc.conf().get(SPARK_OAUTH_APP_SECRET);
                this.authority = sc.conf().get(SPARK_OAUTH_AUTHORITY);
            } else {
                logger.error("OAuth credentials could neither be obtained from active spark session nor spark config file");
            }
        }
    }

    /**
     * The handle method implementation checks the instance(s) of the Callback object(s) passed in to retrieve or display the requested information.
     * 
     * @param callbacks
     *            the callbacks
     * @throws IOException
     * @throws UnsupportedCallbackException
     */
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                try {
                    OAuthBearerToken token = getOAuthBearerToken();
                    OAuthBearerTokenCallback oauthCallback = (OAuthBearerTokenCallback) callback;
                    oauthCallback.token(token);
                } catch (InterruptedException ie) {
                    logger.error("Caught Exception from TokenCallback", ie);
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    logger.error("Caught Exception from TokenCallback", e);
                    throw new IOException(e);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private OAuthBearerToken getOAuthBearerToken() throws MalformedURLException, InterruptedException, ExecutionException {
        if (this.aadClient == null) {
            synchronized (this) {
                if (this.aadClient == null) {
                    IClientCredential credential = ClientCredentialFactory.createFromSecret(this.appSecret);
                    this.aadClient = ConfidentialClientApplication.builder(this.appId, credential)
                                                                  .authority(this.authority)
                                                                  .build();
                }
            }
        }

        IAuthenticationResult authResult = this.aadClient.acquireToken(this.aadParameters).get();

        return new OAuthBearerTokenImp(authResult.accessToken(), authResult.expiresOnDate());
    }

    public void close() {
        // NOOP
    }
}
