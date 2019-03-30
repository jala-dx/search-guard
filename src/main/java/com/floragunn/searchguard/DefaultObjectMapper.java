/*
 * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */

package com.floragunn.searchguard;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.elasticsearch.SpecialPermission;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class DefaultObjectMapper {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    
    static {
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        //objectMapper.setSerializationInclusion(Include.NON_EMPTY);
        //objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
    }
    
    //public static final ObjectMapper objectMapperYaml = new ObjectMapper(new YAMLFactory());

    public static <T> T readValue(String string, Class<T> clazz) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return objectMapper.readValue(string, clazz);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws Exception {
                    return objectMapper.writeValueAsString(value);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (JsonProcessingException) e.getCause();
        }

    }

    public static <T> T readValue(String string, TypeReference<T> tr) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return objectMapper.readValue(string, tr);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }

    }

    public static <T> T readValue(String string, JavaType jt) throws IOException {

        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return objectMapper.readValue(string, jt);
                }
            });
        } catch (final PrivilegedActionException e) {
            throw (IOException) e.getCause();
        }
    }

    public static TypeFactory getTypeFactory() {
        return objectMapper.getTypeFactory();
    }

}
