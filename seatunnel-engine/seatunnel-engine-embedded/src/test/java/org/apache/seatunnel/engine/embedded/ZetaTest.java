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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link Zeta} embedded mode entry point. */
public class ZetaTest {

    @Test
    public void testConfigParsing() {
        // Simple HOCON config for testing (SeaTunnel's native format)
        // Note: SeaTunnel source/sink are parsed as lists, not objects
        String config =
                "env {\n"
                        + "  jobMode = \"BATCH\"\n"
                        + "  parallelism = 2\n"
                        + "}\n"
                        + "source {\n"
                        + "  FakeSource {\n"
                        + "    result_table_name = \"fake\"\n"
                        + "    row_num = 10\n"
                        + "    schema = {\n"
                        + "      fields {\n"
                        + "        name = string\n"
                        + "        age = int\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}\n"
                        + "sink {\n"
                        + "  Console {\n"
                        + "    source_table_name = \"fake\"\n"
                        + "  }\n"
                        + "}";

        org.apache.seatunnel.shade.com.typesafe.config.Config parsedConfig =
                org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory.parseString(config);

        // Verify config parsing works correctly - check root level keys
        Assertions.assertTrue(parsedConfig.hasPath("env"));
        Assertions.assertTrue(parsedConfig.hasPath("source"));
        Assertions.assertTrue(parsedConfig.hasPath("sink"));

        // Access env config
        org.apache.seatunnel.shade.com.typesafe.config.Config envConfig =
                parsedConfig.getConfig("env");
        Assertions.assertEquals("BATCH", envConfig.getString("jobMode"));
        Assertions.assertEquals(2, envConfig.getInt("parallelism"));
        Zeta.run(parsedConfig);
    }

    @Test
    public void testZetaClassExists() {
        // Verify Zeta class exists
        Assertions.assertNotNull(Zeta.class);
    }
}
