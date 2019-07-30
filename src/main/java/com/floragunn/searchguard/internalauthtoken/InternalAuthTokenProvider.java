package com.floragunn.searchguard.internalauthtoken;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.Set;

import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.sgconf.ConfigModel;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory;
import com.floragunn.searchguard.sgconf.DynamicConfigFactory.DCFListener;
import com.floragunn.searchguard.sgconf.DynamicConfigModel;
import com.floragunn.searchguard.sgconf.InternalUsersModel;
import com.floragunn.searchguard.sgconf.SgRoles;
import com.floragunn.searchguard.user.User;
import com.floragunn.searchsupport.util.ObjectTreeXContent;
import com.google.common.base.Strings;

public class InternalAuthTokenProvider implements DCFListener {

    private static final Logger log = LogManager.getLogger(InternalAuthTokenProvider.class);

    //    private DynamicConfigFactory dynamicConfigFactory;
    private JoseJwtProducer jwtProducer;
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
            JsonWebKey signingKey = this.createJwkFromSettings(null); // TODO
            this.jwtProducer = new JoseJwtProducer();
            this.jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(signingKey));
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

}
