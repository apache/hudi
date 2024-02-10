package org.apache.hudi.aws.sync;

import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieAWSConfig;
import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class ITTestGlueUtil extends ITTestSyncUtil {

    protected static final String MOTO_ENDPOINT = "http://localhost:5000";
    public static final String AWS_REGION = "eu-west-1";

    protected static Properties getAwsProperties() {
        Properties hiveProps = new TypedProperties();
        hiveProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
        hiveProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
        hiveProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
        hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), MOTO_ENDPOINT);
        hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), AWS_REGION);
        return hiveProps;
    }

    protected static GlueAsyncClient getGlueAsyncClient(Properties hiveProps) throws URISyntaxException {
      GlueAsyncClient testclient = GlueAsyncClient.builder()
              .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(hiveProps))
              .endpointOverride(new URI(MOTO_ENDPOINT))
              .region(Region.of(AWS_REGION))
              .build();
      return testclient;
    }
    @AfterEach
    public void cleanUp() {
        // drop database and table
    }
}
