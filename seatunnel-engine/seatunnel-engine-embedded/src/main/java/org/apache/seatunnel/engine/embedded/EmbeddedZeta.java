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

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * Embedded Zeta Engine - Enables running SeaTunnel jobs directly within a Java process.
 *
 * <p>This class provides a simple API to start an embedded SeaTunnel Zeta engine and execute jobs
 * programmatically without needing to start a separate server process.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Simple usage with try-with-resources
 * try (EmbeddedZeta zeta = EmbeddedZeta.start()) {
 *     JobResult result = zeta.executeJob("/path/to/config.conf");
 *     System.out.println("Job finished: " + result.getStatus());
 * }
 *
 * // Using builder for custom configuration
 * EmbeddedZeta zeta = EmbeddedZeta.builder()
 *         .clusterName("my-app-engine")
 *         .build();
 *
 * JobResult result = zeta.executeJob("/path/to/config.conf");
 * zeta.close();
 * }</pre>
 */
@Slf4j
public class EmbeddedZeta implements AutoCloseable {

    private final HazelcastInstanceImpl server;
    private final SeaTunnelClient client;
    private final SeaTunnelConfig seaTunnelConfig;
    private volatile boolean closed = false;

    private EmbeddedZeta(@NonNull SeaTunnelConfig config) {
        this.seaTunnelConfig = config;
        log.info("Starting embedded SeaTunnel Zeta engine...");
        // Start embedded server
        this.server = SeaTunnelServerStarter.createHazelcastInstance(config);
        log.info("Embedded SeaTunnel Zeta engine started successfully");

        // Create local client
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(config.getHazelcastConfig().getClusterName());
        // For embedded mode, connect locally without explicit addresses
        clientConfig.getNetworkConfig().setAddresses(Collections.emptyList());

        this.client = new SeaTunnelClient(clientConfig);
        log.info("Embedded client connected to engine");
    }

    /**
     * Starts an embedded Zeta engine with default configuration.
     *
     * <p>This method uses {@link ConfigProvider#locateAndGetSeaTunnelConfig()} to load the
     * configuration.
     *
     * @return a new {@link EmbeddedZeta} instance
     */
    public static EmbeddedZeta start() {
        return start(ConfigProvider.locateAndGetSeaTunnelConfig());
    }

    /**
     * Starts an embedded Zeta engine with the specified configuration.
     *
     * @param config the SeaTunnel configuration to use
     * @return a new {@link EmbeddedZeta} instance
     */
    public static EmbeddedZeta start(@NonNull SeaTunnelConfig config) {
        return new EmbeddedZeta(config);
    }

    /**
     * Starts an embedded Zeta engine with a custom cluster name.
     *
     * <p>This is useful when running multiple embedded engines in the same JVM.
     *
     * @param clusterName the name for the cluster
     * @return a new {@link EmbeddedZeta} instance
     */
    public static EmbeddedZeta start(@NonNull String clusterName) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(clusterName);
        return new EmbeddedZeta(config);
    }

    /**
     * Executes a SeaTunnel job specified by the configuration file.
     *
     * <p>This method will block until the job completes.
     *
     * @param configFile the path to the job configuration file
     * @return the job execution result
     * @throws RuntimeException if the job execution fails
     */
    public JobResult executeJob(@NonNull String configFile) {
        return executeJob(configFile, new JobConfig());
    }

    /**
     * Executes a SeaTunnel job with the specified configuration and job config.
     *
     * <p>This method will block until the job completes.
     *
     * @param configFile the path to the job configuration file
     * @param jobConfig the job configuration options
     * @return the job execution result
     * @throws RuntimeException if the job execution fails
     */
    public JobResult executeJob(@NonNull String configFile, @NonNull JobConfig jobConfig) {
        ensureNotClosed();
        try {
            log.info("Executing job from config file: {}", configFile);
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(configFile, jobConfig, seaTunnelConfig);

            ClientJobProxy jobProxy = env.execute();
            JobResult result = jobProxy.waitForJobCompleteV2();

            log.info(
                    "Job execution completed. Job ID: {}, Status: {}",
                    jobProxy.getJobId(),
                    result.getStatus());
            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Job execution interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to execute job: " + configFile, e);
        }
    }

    /**
     * Executes a SeaTunnel job asynchronously, returning the job proxy for monitoring.
     *
     * <p>This method submits the job and returns immediately. Use the returned {@link
     * ClientJobProxy} to monitor job progress and wait for completion if needed.
     *
     * @param configFile the path to the job configuration file
     * @return the job proxy for monitoring
     * @throws RuntimeException if the job submission fails
     */
    public ClientJobProxy executeJobAsync(@NonNull String configFile) {
        return executeJobAsync(configFile, new JobConfig());
    }

    /**
     * Executes a SeaTunnel job asynchronously with the specified job config.
     *
     * <p>This method submits the job and returns immediately. Use the returned {@link
     * ClientJobProxy} to monitor job progress and wait for completion if needed.
     *
     * @param configFile the path to the job configuration file
     * @param jobConfig the job configuration options
     * @return the job proxy for monitoring
     * @throws RuntimeException if the job submission fails
     */
    public ClientJobProxy executeJobAsync(
            @NonNull String configFile, @NonNull JobConfig jobConfig) {
        ensureNotClosed();
        try {
            log.info("Submitting async job from config file: {}", configFile);
            ClientJobExecutionEnvironment env =
                    client.createExecutionContext(configFile, jobConfig, seaTunnelConfig);

            ClientJobProxy jobProxy = env.execute();
            log.info("Async job submitted. Job ID: {}", jobProxy.getJobId());
            return jobProxy;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Job submission interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job: " + configFile, e);
        }
    }

    /**
     * Gets the underlying JobClient for advanced operations.
     *
     * <p>This allows access to lower-level operations like:
     *
     * <ul>
     *   <li>Getting job status
     *   <li>Cancelling jobs
     *   <li>Querying job metrics
     *   <li>Checkpoint operations
     * </ul>
     *
     * @return the job client
     */
    public JobClient getJobClient() {
        ensureNotClosed();
        return client.createJobClient();
    }

    /**
     * Gets the underlying Hazelcast instance.
     *
     * <p>This provides access to Hazelcast-specific features like distributed data structures.
     *
     * @return the Hazelcast instance
     */
    public HazelcastInstanceImpl getHazelcastInstance() {
        ensureNotClosed();
        return server;
    }

    /**
     * Gets the SeaTunnel configuration used by this engine.
     *
     * @return the SeaTunnel configuration
     */
    public SeaTunnelConfig getSeaTunnelConfig() {
        return seaTunnelConfig;
    }

    /**
     * Checks if this embedded engine is still running.
     *
     * @return true if the engine is running, false otherwise
     */
    public boolean isRunning() {
        return !closed && server != null;
    }

    /**
     * Gets the underlying SeaTunnelServer service.
     *
     * <p>This provides access to server-level operations and services.
     *
     * @return the SeaTunnelServer service
     */
    public SeaTunnelServer getSeaTunnelServer() {
        ensureNotClosed();
        // node is a public field in HazelcastInstanceImpl
        return server.node.getNodeEngine().getService(Constant.SEATUNNEL_SERVICE_NAME);
    }

    /**
     * Closes the embedded engine and releases all resources.
     *
     * <p>This method shuts down the client and server instances. Once closed, this engine cannot be
     * reused.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        log.info("Shutting down embedded SeaTunnel Zeta engine...");

        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.warn("Error closing client", e);
        }

        try {
            if (server != null) {
                server.shutdown();
            }
        } catch (Exception e) {
            log.warn("Error shutting down server", e);
        }

        log.info("Embedded SeaTunnel Zeta engine shut down complete");
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("EmbeddedZeta has been closed");
        }
    }

    /**
     * Creates a new builder for configuring and building an EmbeddedZeta instance.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for creating configured {@link EmbeddedZeta} instances. */
    public static class Builder {
        private SeaTunnelConfig config;
        private String clusterName;
        private Integer port;
        private Boolean liteMember;

        /**
         * Sets the SeaTunnel configuration to use.
         *
         * @param config the configuration
         * @return this builder
         */
        public Builder config(@NonNull SeaTunnelConfig config) {
            this.config = config;
            return this;
        }

        /**
         * Sets the cluster name for the embedded engine.
         *
         * <p>Useful when running multiple embedded engines in the same JVM.
         *
         * @param clusterName the cluster name
         * @return this builder
         */
        public Builder clusterName(@NonNull String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        /**
         * Sets the network port for the embedded engine.
         *
         * <p>If not specified, a default port will be used.
         *
         * @param port the network port
         * @return this builder
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Sets whether this node should be a lite member (dataless member).
         *
         * <p>Lite members do not store data and are typically used for compute-only scenarios.
         *
         * @param liteMember true to make this a lite member
         * @return this builder
         */
        public Builder liteMember(boolean liteMember) {
            this.liteMember = liteMember;
            return this;
        }

        /**
         * Builds the {@link EmbeddedZeta} instance with the configured options.
         *
         * @return a new EmbeddedZeta instance
         */
        public EmbeddedZeta build() {
            SeaTunnelConfig finalConfig =
                    config != null ? config : ConfigProvider.locateAndGetSeaTunnelConfig();

            Config hazelcastConfig = finalConfig.getHazelcastConfig();

            if (clusterName != null) {
                hazelcastConfig.setClusterName(clusterName);
            }

            if (port != null) {
                hazelcastConfig.getNetworkConfig().setPort(port);
            }

            if (liteMember != null) {
                hazelcastConfig.setLiteMember(liteMember);
            }

            return new EmbeddedZeta(finalConfig);
        }
    }
}
