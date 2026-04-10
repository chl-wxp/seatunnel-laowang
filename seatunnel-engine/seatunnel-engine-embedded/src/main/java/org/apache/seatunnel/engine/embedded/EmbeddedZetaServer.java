/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.embedded;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class EmbeddedZetaServer implements AutoCloseable {

    private final HazelcastInstanceImpl server;
    private final SeaTunnelConfig seaTunnelConfig;
    private final NodeEngineImpl nodeEngine;

    protected EmbeddedZetaServer(@NonNull SeaTunnelConfig config) {
        this.seaTunnelConfig = config;
        log.info("Starting embedded SeaTunnel Zeta engine...");
        // Start embedded server
        this.server = SeaTunnelServerStarter.createHazelcastInstance(config);
        this.nodeEngine = server.node.getNodeEngine();
        log.info("Embedded SeaTunnel Zeta engine started successfully");
    }

    /**
     * Executes a SeaTunnel job specified by the Config object directly.
     *
     * <p>This method will block until the job completes.
     *
     * @param config the job configuration object (Typesafe Config)
     * @return the job execution result
     * @throws RuntimeException if the job execution fails
     */
    public JobResult executeJob(@NonNull Config config) {
        return executeJob(config, new JobConfig());
    }

    /**
     * Executes a SeaTunnel job with the specified Config object and job config.
     *
     * <p>This method will block until the job completes.
     *
     * @param config the job configuration object (Typesafe Config)
     * @param jobConfig the job configuration options
     * @return the job execution result
     * @throws RuntimeException if the job execution fails
     */
    public JobResult executeJob(@NonNull Config config, @NonNull JobConfig jobConfig) {
        try {
            log.info("Executing job from Config object");
            EmbeddedJobExecutionEnvironment env =
                    new EmbeddedJobExecutionEnvironment(
                            jobConfig, config, seaTunnelConfig, nodeEngine);

            JobImmutableInformation jobImmutableInformation = env.build();
            long jobId = jobImmutableInformation.getJobId();

            // Submit job
            submitJob(jobImmutableInformation);

            // Wait for job completion
            JobResult result = waitForJobCompletion(jobId);

            log.info("Job execution completed. Job ID: {}, Status: {}", jobId, result.getStatus());
            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Job execution interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to execute job from Config", e);
        }
    }

    private void submitJob(JobImmutableInformation jobImmutableInformation) {
        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();

        PassiveCompletableFuture<Void> future =
                coordinatorService.submitJob(
                        jobImmutableInformation.getJobId(),
                        nodeEngine.toData(jobImmutableInformation),
                        jobImmutableInformation.isStartWithSavePoint());
        future.join();
    }

    private JobResult waitForJobCompletion(long jobId)
            throws InterruptedException, ExecutionException {
        log.info("Waiting for job {} to complete", jobId);

        Map<String, Object> extensionServices =
                nodeEngine.getNode().getNodeExtension().createExtensionServices();
        SeaTunnelServer seaTunnelServer =
                (SeaTunnelServer) extensionServices.get(Constant.SEATUNNEL_SERVICE_NAME);
        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();

        // Poll for job completion
        long startTime = System.currentTimeMillis();
        long timeoutMillis = TimeUnit.HOURS.toMillis(24); // 24 hour timeout
        long pollIntervalMillis = 500; // 500ms poll interval

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            JobStatus status = coordinatorService.getJobStatus(jobId);

            if (status != null && status.isEndState()) {
                log.info("Job {} completed with status {}", jobId, status);
                return new JobResult(status);
            }

            Thread.sleep(pollIntervalMillis);
        }

        throw new ExecutionException(
                "Job " + jobId + " did not complete within " + timeoutMillis + "ms",
                new java.util.concurrent.TimeoutException());
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdown();
        }
        log.info("Embedded SeaTunnel Zeta engine shut down complete");
    }
}
