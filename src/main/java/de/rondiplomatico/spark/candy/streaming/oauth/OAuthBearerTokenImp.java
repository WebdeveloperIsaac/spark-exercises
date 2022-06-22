package de.rondiplomatico.spark.candy.streaming.oauth;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

/**
 * Object holding the OAuth bearer token with the token's lifetime.
 */
public class OAuthBearerTokenImp implements OAuthBearerToken {

    String token;
    long lifetimeMs;

    /**
     * Constructor.
     * 
     * @param token
     *            bearer token
     * @param expiresOn
     *            expiration date
     */
    public OAuthBearerTokenImp(final String token, Date expiresOn) {
        this.token = token;
        this.lifetimeMs = expiresOn.getTime();
    }

    @Override
    public String value() {
        return this.token;
    }

    @Override
    public Set<String> scope() {
        return Collections.emptySet();
    }

    @Override
    public long lifetimeMs() {
        return this.lifetimeMs;
    }

    @Override
    public String principalName() {
        return null;
    }

    @Override
    public Long startTimeMs() {
        return null;
    }
}
