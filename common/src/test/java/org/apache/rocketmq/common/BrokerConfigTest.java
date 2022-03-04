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
package org.apache.rocketmq.common;

import org.junit.jupiter.api.Test;



public class BrokerConfigTest {

    @Test
    void testConsumerFallBehindThresholdOverflow() {
        long expect = 1024L * 1024 * 1024 * 16;
        Assertions.assertEquals(new BrokerConfig().getConsumerFallbehindThreshold(),expect);
    }

    @Test
    void testBrokerConfigAttribute() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr("127.0.0.1:9876");
        brokerConfig.setAutoCreateTopicEnable(false);
        brokerConfig.setBrokerName("broker-a");
        brokerConfig.setBrokerId(0);
        brokerConfig.setBrokerClusterName("DefaultCluster");
        brokerConfig.setMsgTraceTopicName("RMQ_SYS_TRACE_TOPIC4");
        brokerConfig.setAutoDeleteUnusedStats(true);
        Assertions.assertEquals(brokerConfig.getBrokerClusterName(),"DefaultCluster");
        Assertions.assertEquals(brokerConfig.getNamesrvAddr(),"127.0.0.1:9876");
        Assertions.assertEquals(brokerConfig.getMsgTraceTopicName(),"RMQ_SYS_TRACE_TOPIC4");
        Assertions.assertEquals(brokerConfig.getBrokerId(),0);
        Assertions.assertEquals(brokerConfig.getBrokerName(),"broker-a");
        Assertions.assertEquals(brokerConfig.isAutoCreateTopicEnable(),false);
        Assertions.assertEquals(brokerConfig.isAutoDeleteUnusedStats(),true);
    }
}