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

import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;

/** Unit tests for {@link Zeta} embedded mode entry point. */
public class ZetaTest {

    @Test
    public void testZetaRun() {
        URL url =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("fake_to_console_batch.conf");
        Assertions.assertNotNull(url);
        final String path = url.getPath();
        final JobResult jobResult = Zeta.run(path);
        Assertions.assertNull(jobResult.getError());
        Assertions.assertEquals(JobStatus.FINISHED, jobResult.getStatus());
    }
}
