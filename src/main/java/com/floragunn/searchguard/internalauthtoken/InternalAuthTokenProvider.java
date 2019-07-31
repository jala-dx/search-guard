package com.floragunn.searchguard.internalauthtoken;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Map;
import java.util.Set;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.ConfigModelV7;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.sgconf.impl.SgDynamicConfiguration;
import com.floragunn.searchguard.sgconf.impl.v7.RoleV7;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.HeaderHelper;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ObjectTreeXContent;
import com.google.common.base.Strings;

public class InternalAuthTokenProvider implements DCFListener {

    public static final String TOKEN_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token";
    public static final String AUDIENCE_HEADER = ConfigConstants.SG_CONFIG_PREFIX + "internal_auth_token_audience";

    private static final Logger log = LogManager.getLogger(InternalAuthTokenProvider.class);

    //    private DynamicConfigFactory dynamicConfigFactory;
    private JsonWebKey signingKey;
    private JoseJwtProducer jwtProducer;
    private JwsSignatureVerifier jwsSignatureVerifier;
    private ConfigModel configModel;
    private SgRoles sgRoles;

    public InternalAuthTokenProvider(DynamicConfigFactory dynamicConfigFactory) {
        dynamicConfigFactory.registerDCFListener(this);
        //      this.dynamicConfigFactory = dynamicConfigFactory;
    }

    public String getJwt(User user, String aud) throws IllegalStateException {
        return getJwt(user, aud, null);
    }

    public String getJwt(User user, String aud, TemporalAmount validity) throws IllegalStateException {

        if (jwtProducer == null) {
            throw new IllegalStateException("AuthTokenProvider is not configured");
        }

        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);
        Instant now = Instant.now();

        jwtClaims.setNotBefore(now.getEpochSecond() - 30);

        if (validity != null) {
            jwtClaims.setExpiryTime(now.plus(validity).getEpochSecond());
        }

        jwtClaims.setSubject(user.getName());
        jwtClaims.setAudience(aud);
        jwtClaims.setProperty("sg_roles", getSgRolesForUser(user));

        String encodedJwt = this.jwtProducer.processJwt(jwt);

        return encodedJwt;
    }

    public AuthFromInternalAuthToken userAuthFromToken(ThreadContext threadContext) {
        final String authToken = threadContext.getHeader(TOKEN_HEADER);
        final String authTokenAudience = HeaderHelper.getSafeFromHeader(threadContext, AUDIENCE_HEADER); 
        
        if (authToken == null || authTokenAudience == null) {
            return null;
        }
        
        return userAuthFromToken(authToken, authTokenAudience);
    }

    
    public AuthFromInternalAuthToken userAuthFromToken(String authToken, String authTokenAudience) {
        try {
            JwtToken verifiedToken = getVerifiedJwtToken(authToken, authTokenAudience);

            Map<String, Object> rolesMap = verifiedToken.getClaims().getMapProperty("sg_roles");
            
            if (rolesMap == null) {
                throw new JwtException("JWT does not contain claim sg_roles");
            }
            
            SgDynamicConfiguration<?> rolesConfig = SgDynamicConfiguration.fromMap(rolesMap, CType.ROLES);
            
            // TODO support v6 xconfig
            SgRoles sgRoles = ConfigModelV7.SgRoles.create((SgDynamicConfiguration<RoleV7>) rolesConfig, configModel.getActionGroupResolver());
            String userName = verifiedToken.getClaims().getSubject();
            User user = new User(userName, sgRoles.getRoleNames(), new AuthCredentials(userName, authToken));
            AuthFromInternalAuthToken userAuth = new AuthFromInternalAuthToken(user, sgRoles);
            
            return userAuth;
                       
        } catch (Exception e) {
            log.warn("Error while verifying internal auth token: " + authToken + "\n" + authTokenAudience, e);
            
            return null;
        }
    }

    @Override
    public void onChanged(ConfigModel configModel, DynamicConfigModel dynamicConfigModel, InternalUsersModel internalUsersModel) {
        this.configModel = configModel;
        this.sgRoles = configModel.getSgRoles();

        initJwtProducer(dynamicConfigModel);
    }

    JsonWebKey createJwkFromSettings(Settings settings) throws Exception {

        String signingKey = "blabla"; // TODO settings.get("jwt.signing_key");

        if (Strings.isNullOrEmpty(signingKey)) {
            throw new Exception("No signing key is configured.");
        }

        JsonWebKey jwk = new JsonWebKey();

        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("HS512");
        jwk.setPublicKeyUse(PublicKeyUse.SIGN);
        jwk.setProperty("k", signingKey);

        return jwk;
    }

    void initJwtProducer(DynamicConfigModel dynamicConfigModel) {
        try {
            this.signingKey = this.createJwkFromSettings(null); // TODO
            this.jwtProducer = new JoseJwtProducer();
            this.jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(signingKey));
            this.jwsSignatureVerifier = JwsUtils.getSignatureVerifier(this.signingKey);

        } catch (Exception e) {
            this.jwtProducer = null;
            log.error("Error while initializing JWT producer in AuthTokenProvider", e);
        }
    }

    private Object getSgRolesForUser(User user) {
        try {
            Set<String> sgRoles = this.configModel.mapSgRoles(user, null);

            SgRoles userRoles = this.sgRoles.filter(sgRoles);

            return ObjectTreeXContent.toObjectTree(userRoles);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JwtToken getVerifiedJwtToken(String encodedJwt, String authTokenAudience) throws JwtException {
        JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
        JwtToken jwt = jwtConsumer.getJwtToken();

        boolean signatureValid = jwtConsumer.verifySignatureWith(jwsSignatureVerifier);

        if (!signatureValid) {
            throw new JwtException("Invalid JWT signature");
        }

        validateClaims(jwt, authTokenAudience);

        return jwt;

    }

    private void validateClaims(JwtToken jwt, String authTokenAudience) throws JwtException {
        JwtClaims claims = jwt.getClaims();

        if (claims == null) {
            throw new JwtException("The JWT does not have any claims");
        }

        JwtUtils.validateJwtExpiry(claims, 0, false);
        JwtUtils.validateJwtNotBefore(claims, 0, false);
        validateAudience(claims, authTokenAudience);

    }

    private void validateAudience(JwtClaims claims, String authTokenAudience) throws JwtException {

        if (authTokenAudience != null) {
            for (String audience : claims.getAudiences()) {
                if (authTokenAudience.equals(audience)) {
                    return;
                }
            }
        }
        throw new JwtException("Internal auth token does not allow audience: " + authTokenAudience + "\nAllowed audiences: " + claims.getAudiences());
    }
    
    public static class AuthFromInternalAuthToken {
        
        private final User user;
        private final SgRoles sgRoles;
        
        AuthFromInternalAuthToken(User user, SgRoles sgRoles) {
            this.user = user;
            this.sgRoles = sgRoles;
        }

        public User getUser() {
            return user;
        }

        public SgRoles getSgRoles() {
            return sgRoles;
        }

        @Override
        public String toString() {
            return "AuthFromInternalAuthToken [user=" + user + ", sgRoles=" + sgRoles + "]";
        }
    }
}
