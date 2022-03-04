/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.latency;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class LatencyFaultToleranceImplTest {
    private LatencyFaultTolerance<String> latencyFaultTolerance;
    private String brokerName = "BrokerA";
    private String anotherBrokerName = "BrokerB";

    @BeforeEach
    public void init() {
        latencyFaultTolerance = new LatencyFaultToleranceImpl();
    }

    @Test
    void testUpdateFaultItem() throws Exception {
        latencyFaultTolerance.updateFaultItem(brokerName, 3000, 3000);
        Assertions.assertFalse(latencyFaultTolerance.isAvailable(brokerName));
        Assertions.assertTrue(latencyFaultTolerance.isAvailable(anotherBrokerName));
    }

    @Test
    void testIsAvailable() throws Exception {
        latencyFaultTolerance.updateFaultItem(brokerName, 3000, 50);
        Assertions.assertFalse(latencyFaultTolerance.isAvailable(brokerName));

        TimeUnit.MILLISECONDS.sleep(70);
        Assertions.assertTrue(latencyFaultTolerance.isAvailable(brokerName));
    }

    @Test
    void testRemove() throws Exception {
        latencyFaultTolerance.updateFaultItem(brokerName, 3000, 3000);
        Assertions.assertFalse(latencyFaultTolerance.isAvailable(brokerName));
        latencyFaultTolerance.remove(brokerName);
        Assertions.assertTrue(latencyFaultTolerance.isAvailable(brokerName));
    }

    @Test
    void testPickOneAtLeast() throws Exception {
        latencyFaultTolerance.updateFaultItem(brokerName, 1000, 3000);
        Assertions.assertEquals(latencyFaultTolerance.pickOneAtLeast(),brokerName);

        latencyFaultTolerance.updateFaultItem(anotherBrokerName, 1001, 3000);
        Assertions.assertEquals(latencyFaultTolerance.pickOneAtLeast(),brokerName);
    }
}