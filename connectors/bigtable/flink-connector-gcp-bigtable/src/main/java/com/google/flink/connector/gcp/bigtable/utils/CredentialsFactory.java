/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.flink.connector.gcp.bigtable.utils;

import org.apache.flink.annotation.PublicEvolving;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Optional;

/** Factory to create {@link GoogleCredentials} configurations. */
@AutoValue
@PublicEvolving
public abstract class CredentialsFactory implements Serializable {

    @Nullable
    public abstract String credentialsFile();

    @Nullable
    public abstract String credentialsKey();

    @Nullable
    public abstract String accessToken();

    /** Builder for the {@code CredentialFactory}. */
    @AutoValue.Builder
    public abstract static class Builder {

        /** Sets the credentials using a file system location. */
        public abstract Builder setCredentialsFile(String credentialsFile);

        /** Sets the credentials using a credentials key, encoded in Base64. */
        public abstract Builder setCredentialsKey(String credentialsKey);

        /** Sets the credentials using a GCP access token. */
        public abstract Builder setAccessToken(String credentialsToken);

        /** Builds a fully initialized {@code CredentialsFactory} instance. */
        public abstract CredentialsFactory build();
    }

    public static CredentialsFactory.Builder builder() {
        return new AutoValue_CredentialsFactory.Builder();
    }

    /**
     * Returns the Google Credentials created given the provided configuration or an empty Optional.
     */
    public Optional<GoogleCredentials> getCredentialsOr() {
        try {
            if (accessToken() != null) {
                return Optional.of(
                        GoogleCredentials.create(
                                AccessToken.newBuilder().setTokenValue(accessToken()).build()));
            } else if (credentialsKey() != null) {
                return Optional.of(
                        GoogleCredentials.fromStream(
                                new ByteArrayInputStream(
                                        Base64.getDecoder().decode(credentialsKey()))));
            } else if (credentialsFile() != null) {
                return Optional.of(
                        GoogleCredentials.fromStream(new FileInputStream(credentialsFile())));
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials", e);
        }
    }
}
