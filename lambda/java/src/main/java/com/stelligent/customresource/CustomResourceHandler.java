package com.stelligent.customresource;

import com.amazon.simpsonconfigpublisher.lambda.accessor.SimpsonConfigServiceAccessor;
import com.amazon.simpsonconfigpublisher.lambda.domain.CloudFormationRequest;
import com.amazon.simpsonconfigpublisher.lambda.domain.ConfigurationCreationContext;
import com.amazon.simpsonconfigpublisher.lambda.domain.CreateConfigInput;
import com.amazon.simpsonconfigpublisher.lambda.domain.CreateConfigOutput;
import com.amazon.simpsonconfigpublisher.lambda.helper.CloudFormationHelper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * This Lambda is responsible for publishing the latest version of the configuration document into the SimpsonConfigService.
 *
 * As a part of the GEAR project, LMP is moving towards making the publishing of tenant configurations a self serve
 * process. Every business program has a Brazil package that contains the configurations for the tenants under that
 * business program. Every change to the configurations package results in the package being built into a CDK managed
 * pipeline that has multiple deployment groups - (beta, gamma and prod across multiple retail regions). In every
 * deployment group, there is a Cloudformation custom resource backed by this Lambda. This custom resource is built
 * on every deployment and results in a call to this Lambda.
 *
 * As a part of the BATS packaging step in the CDK managed pipeline, the zipped tenant config and manifest files
 * are written to the deployment bucket. This Lambda gets the manifest from the S3 bucket, constructs the S3 keys
 * for the tenant zipped archives specific to a domain and realm combination and calls SCS to upload each archive.
 *
 * Once all the tenant configurations are uploaded, the Lambda then sends a successful response to CFN, completing
 * the deployment.
 */
@Log4j2
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ConfigurationPublisher implements RequestHandler<Map<String, Object>, Object> {

    private static final CloudFormationHelper CFN_HELPER = new CloudFormationHelper();
    public static final String S3_KEY_FORMAT = "%s/%s_%s/%s";
    public static final String S3_KEY_SUFFIX = "packaging_additional_published_artifacts/tenant_zipped_config/test/" +
            "USAmazon/US_TST.zip";

    private final SimpsonConfigServiceAccessor simpsonConfigServiceAccessor;

    @SneakyThrows
    @Override
    public Object handleRequest(Map<String, Object> stringObjectMap, Context context) {
        //Extract the CFN input data.
        String status;
        CloudFormationRequest cloudFormationRequest = CFN_HELPER.parseInput(stringObjectMap);
        try {
            log.info("Received input from CFN : " + cloudFormationRequest.toString());

            switch (cloudFormationRequest.getRequestType()) {
                case CloudFormationHelper.CFN_REQUEST_TYPE_CREATE:
                case CloudFormationHelper.CFN_REQUEST_TYPE_UPDATE:
                    CreateConfigOutput output = createConfig(cloudFormationRequest);
                    log.info("Success. New configuration version "+output.getVersion());
                    status = CloudFormationHelper.CFN_STATUS_SUCCESS;
                    sendResponse(cloudFormationRequest, status, status);
                    break;
                case CloudFormationHelper.CFN_REQUEST_TYPE_DELETE:
                    log.warn("Received delete request from CFN. Not doing anything.");
                    status = CloudFormationHelper.CFN_STATUS_SUCCESS;
                    sendResponse(cloudFormationRequest, status, status);
                    break;
                default:
                    log.error("Unsupported CFN request type --> "+cloudFormationRequest.getRequestType());
                    status = CloudFormationHelper.CFN_STATUS_FAILED;
                    sendResponse(cloudFormationRequest, status, "Unsupported CFN request type");
                    break;
            }
        } catch (Exception e) {
            log.error(e);
            status = CloudFormationHelper.CFN_STATUS_FAILED;
            sendResponse(cloudFormationRequest, status, "Failed due to unknown error");
        }
        return status;
    }

    private CreateConfigOutput createConfig(CloudFormationRequest request) {
        ConfigurationCreationContext context = request.getConfigurationCreationContext();
        String s3Key = String.format(S3_KEY_FORMAT, context.getS3KeyPrefix(), context.getPackagingAggregateId(),
                context.getTenantConfigTransformName(), S3_KEY_SUFFIX);
        String s3Bucket = context.getBucketName();
        log.info("Constructed key "+s3Key+" for bucket "+s3Bucket);
        return simpsonConfigServiceAccessor.createConfiguration(
                CreateConfigInput.builder()
                        .tenantId("US_TST")
                        .s3Key(s3Key)
                        .s3BucketName(s3Bucket)
                        .context("LIVE")
                        .metadata(ImmutableMap.<String, String>builder()
                                .put(CloudFormationHelper.PACKAGING_AGG_ID, context.getPackagingAggregateId())
                                .build())
                        .build());
    }

    private void sendResponse(CloudFormationRequest input, String status, String reason) throws IOException {
        URL url = new URL(input.getResponseUrl());
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod(CloudFormationHelper.HTTP_REQUEST_METHOD);
        postResponse(connection, status, reason, input);
    }

    private void postResponse(HttpURLConnection connection, String status, String reason,
                              CloudFormationRequest cloudFormationRequest)
            throws IOException {
        try (OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), StandardCharsets.UTF_8)) {
            JsonObject responseBody = new JsonObject();
            responseBody.addProperty(CloudFormationHelper.STATUS_KEY, status);
            responseBody.addProperty(CloudFormationHelper.REASON, reason);
            responseBody.addProperty(CloudFormationHelper.PHYSICAL_RESOURCE_ID,
                    cloudFormationRequest.getPhysicalResourceId());
            responseBody.addProperty(CloudFormationHelper.STACK_ID, cloudFormationRequest.getStackId());
            responseBody.addProperty(CloudFormationHelper.REQUEST_ID, cloudFormationRequest.getRequestId());
            responseBody.addProperty(CloudFormationHelper.LOGICAL_RESOURCE_ID,
                    cloudFormationRequest.getLogicalResourceId());

            log.info("Sending the CFN response "+ responseBody.toString());

            out.write(responseBody.toString());

            log.info(String.format("Response Code for Request: %s is: %s", cloudFormationRequest.getRequestId(),
                    connection.getResponseCode()));
        }
    }
}
