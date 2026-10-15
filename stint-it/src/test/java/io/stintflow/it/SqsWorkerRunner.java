package io.stintflow.it;

import java.time.Duration;

import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * The ITs' worker over SQS: polls the invoke queue, runs the task via {@link WorkerRuntime}, publishes the result to
 * the result queue and deletes the message after. Since SDD 3a.7 it is the real {@link SqsWorkerBinding} — kept to one
 * delivery at a time and a 2 s long poll, as the serial prototype it replaces, so the older ITs keep their semantics.
 */
final class SqsWorkerRunner implements AutoCloseable {

    private final SqsWorkerBinding binding;

    SqsWorkerRunner(SqsClient sqs, String invokeQueueUrl, String resultQueueUrl, WorkerRuntime worker) {
        binding = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invokeQueueUrl).resultQueueUrl(resultQueueUrl)
                .runtime(worker).concurrency(1).waitTime(Duration.ofSeconds(2)).shutdownGrace(Duration.ofSeconds(5))
                .build().start();
    }

    @Override
    public void close() {
        binding.close();
    }
}
