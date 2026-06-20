package io.stintflow.bundle.aws;

/**
 * Marker for the AWS bundle (core + AWS connectors). Depend on this artifact to pull in the
 * S3/DynamoDB/SQS/SNS/EventBridge stack. The class itself carries no behaviour.
 */
public final class AwsBundle {
    private AwsBundle() {
    }
}
