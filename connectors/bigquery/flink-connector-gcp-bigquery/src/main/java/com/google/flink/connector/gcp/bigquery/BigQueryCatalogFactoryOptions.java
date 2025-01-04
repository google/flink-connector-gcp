
package com.google.flink.connector.gcp.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** {@link ConfigOption}s for BigQueryCatalog. */
@Internal
public final class BigQueryCatalogFactoryOptions {

    public static final String IDENTIFIER = "bigquery";

    public static final ConfigOption<String> DEFAULT_DATASET =
            ConfigOptions.key("default-dataset")
                    .stringType()
                    .defaultValue("default");

    public static final ConfigOption<String> BIGQUERY_PROJECT =
            ConfigOptions.key("bigquery-project")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> CREDENTIAL_FILE =
            ConfigOptions.key("credential-file")
                    .stringType()
                    .noDefaultValue();

}