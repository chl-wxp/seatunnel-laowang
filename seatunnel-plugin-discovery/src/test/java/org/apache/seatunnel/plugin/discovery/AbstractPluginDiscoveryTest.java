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

package org.apache.seatunnel.plugin.discovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class AbstractPluginDiscoveryTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private static final String seatunnelHome;

    static {
        String rootModuleDir = "seatunnel-plugin-discovery";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(rootModuleDir))) {
            path = path.getParent();
        }
        seatunnelHome =
                Paths.get(
                                path.getParent().toString(),
                                rootModuleDir,
                                "target",
                                "test-classes",
                                "home")
                        .toString();
    }

    @BeforeEach
    public void before() {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);
    }

    @Test
    public void testGetAllPlugins() {
        Map<PluginIdentifier, String> sourcePlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SOURCE);
        Assertions.assertEquals(30, sourcePlugins.size());

        Map<PluginIdentifier, String> sinkPlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SINK);
        Assertions.assertEquals(34, sinkPlugins.size());
    }

    @AfterEach
    public void after() {
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
    }

    @Test
    public void testLoadConnectorPluginConfigFallbackToClasspath(@TempDir Path tempDir) {
        // Setup: Create empty connector dir (no plugin-mapping.properties)
        Path connectorDir = tempDir.resolve("connectors");
        connectorDir.toFile().mkdirs();
        Common.setSeaTunnelHome(tempDir.toString());

        // Test: Should fallback to classpath when connector dir has no mapping file
        Config config = TestPluginDiscovery.callLoadConnectorPluginConfig();

        // Verify: Should load from classpath (test resources)
        Assertions.assertNotNull(config);
        Assertions.assertFalse(config.isEmpty());
        Assertions.assertTrue(config.hasPath("seatunnel"));

        // Verify: getAllSupportedPlugins should load from classpath fallback
        Map<PluginIdentifier, String> sourcePlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SOURCE);
        Assertions.assertEquals(1, sourcePlugins.size());

        Map<PluginIdentifier, String> sinkPlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SINK);
        Assertions.assertEquals(1, sinkPlugins.size());
    }

    /** Test subclass to access protected {@code loadConnectorPluginConfig()} method. */
    private static class TestPluginDiscovery extends AbstractPluginDiscovery<Object> {
        public TestPluginDiscovery(Path pluginDir) {
            super(pluginDir);
        }

        @Override
        protected Class<Object> getPluginBaseClass() {
            return Object.class;
        }

        public static Config callLoadConnectorPluginConfig() {
            return loadConnectorPluginConfig();
        }
    }
}
