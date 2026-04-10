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

import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;

@Slf4j
public class Zeta {

    /**
     * Run a SeaTunnel job directly from a Config object.
     *
     * @param config the job configuration object
     * @return the job execution result
     */
    public static synchronized JobResult run(Config config) {
        SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();
        try (EmbeddedZetaServer embeddedZetaServer = new EmbeddedZetaServer(seaTunnelConfig)) {
            return embeddedZetaServer.executeJob(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run SeaTunnel job", e);
        }
    }

    /**
     * Run a SeaTunnel job from a configuration file path.
     *
     * @param configFilePath the path to the job configuration file
     * @return the job execution result
     */
    public static synchronized JobResult run(String configFilePath) {
        return run(ConfigBuilder.of(Paths.get(configFilePath)));
    }
}
