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

package stats;

import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.rocketmq.store.stats.BrokerStatsManager.BROKER_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_FALL_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_FALL_TIME;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_LATENCY;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.GROUP_GET_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.QUEUE_GET_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.QUEUE_GET_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.QUEUE_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.QUEUE_PUT_SIZE;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.SNDBCK_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.TOPIC_PUT_NUMS;
import static org.apache.rocketmq.store.stats.BrokerStatsManager.TOPIC_PUT_SIZE;


public class BrokerStatsManagerTest {
    private BrokerStatsManager brokerStatsManager;

    private String TOPIC = "TOPIC_TEST";
    private Integer QUEUE_ID = 0;
    private String GROUP_NAME = "GROUP_TEST";

    @BeforeEach
    public void init() {
        brokerStatsManager = new BrokerStatsManager("DefaultCluster", true);
        brokerStatsManager.start();
    }

    @AfterEach
    public void destory() {
        brokerStatsManager.shutdown();
    }

    @Test
    void testGetStatsItem() {
        Assertions.assertNull(brokerStatsManager.getStatsItem("TEST", "TEST"));
    }

    @Test
    void testIncQueuePutNums() {
        brokerStatsManager.incQueuePutNums(TOPIC, QUEUE_ID);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, String.valueOf(QUEUE_ID));
        Assertions.assertEquals(brokerStatsManager.getStatsItem(QUEUE_PUT_NUMS, statsKey).getTimes().doubleValue(),1L);
        brokerStatsManager.incQueuePutNums(TOPIC, QUEUE_ID, 2, 2);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(QUEUE_PUT_NUMS, statsKey).getValue().doubleValue(),3L);
    }

    @Test
    void testIncQueuePutSize() {
        brokerStatsManager.incQueuePutSize(TOPIC, QUEUE_ID, 2);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, String.valueOf(QUEUE_ID));
        Assertions.assertEquals(brokerStatsManager.getStatsItem(QUEUE_PUT_SIZE, statsKey).getValue().doubleValue(),2L);
    }

    @Test
    void testIncQueueGetNums() {
        brokerStatsManager.incQueueGetNums(GROUP_NAME, TOPIC, QUEUE_ID, 1);
        final String statsKey = brokerStatsManager.buildStatsKey(brokerStatsManager.buildStatsKey(TOPIC, String.valueOf(QUEUE_ID)), GROUP_NAME);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(QUEUE_GET_NUMS, statsKey).getValue().doubleValue(),1L);
    }

    @Test
    void testIncQueueGetSize() {
        brokerStatsManager.incQueueGetSize(GROUP_NAME, TOPIC, QUEUE_ID, 1);
        final String statsKey = brokerStatsManager.buildStatsKey(brokerStatsManager.buildStatsKey(TOPIC, String.valueOf(QUEUE_ID)), GROUP_NAME);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(QUEUE_GET_SIZE, statsKey).getValue().doubleValue(),1L);
    }

    @Test
    void testIncTopicPutNums() {
        brokerStatsManager.incTopicPutNums(TOPIC);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getTimes().doubleValue(),1L);
        brokerStatsManager.incTopicPutNums(TOPIC, 2, 2);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC).getValue().doubleValue(),3L);
    }

    @Test
    void testIncTopicPutSize() {
        brokerStatsManager.incTopicPutSize(TOPIC, 2);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC).getValue().doubleValue(),2L);
    }

    @Test
    void testIncGroupGetNums() {
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, statsKey).getValue().doubleValue(),1L);
    }

    @Test
    void testIncGroupGetSize() {
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 1);
        String statsKey = brokerStatsManager.buildStatsKey(TOPIC, GROUP_NAME);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, statsKey).getValue().doubleValue(),1L);
    }

    @Test
    void testIncGroupGetLatency() {
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        String statsKey = String.format("%d@%s@%s", 1, TOPIC, GROUP_NAME);
        Assertions.assertEquals(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, statsKey).getValue().doubleValue(),1L);
    }

    @Test
    void testIncBrokerPutNums() {
        brokerStatsManager.incBrokerPutNums();
        Assertions.assertEquals(brokerStatsManager.getStatsItem(BROKER_PUT_NUMS, "DefaultCluster").getValue().doubleValue(),1L);
    }

    @Test
    void testOnTopicDeleted() {
        brokerStatsManager.incTopicPutNums(TOPIC);
        brokerStatsManager.incTopicPutSize(TOPIC, 100);
        brokerStatsManager.incQueuePutNums(TOPIC, QUEUE_ID);
        brokerStatsManager.incQueuePutSize(TOPIC, QUEUE_ID, 100);
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 100);
        brokerStatsManager.incQueueGetNums(GROUP_NAME, TOPIC, QUEUE_ID, 1);
        brokerStatsManager.incQueueGetSize(GROUP_NAME, TOPIC, QUEUE_ID, 100);
        brokerStatsManager.incSendBackNums(GROUP_NAME, TOPIC);
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        brokerStatsManager.recordDiskFallBehindTime(GROUP_NAME, TOPIC, 1, 11L);
        brokerStatsManager.recordDiskFallBehindSize(GROUP_NAME, TOPIC, 1, 11L);

        brokerStatsManager.onTopicDeleted(TOPIC);

        Assertions.assertNull(brokerStatsManager.getStatsItem(TOPIC_PUT_NUMS, TOPIC));
        Assertions.assertNull(brokerStatsManager.getStatsItem(TOPIC_PUT_SIZE, TOPIC));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_PUT_NUMS, TOPIC + "@" + QUEUE_ID));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_PUT_SIZE, TOPIC + "@" + QUEUE_ID));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_GET_SIZE, TOPIC + "@" + QUEUE_ID + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_GET_NUMS, TOPIC + "@" + QUEUE_ID + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(SNDBCK_PUT_NUMS, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, "1@" + TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_SIZE, "1@" + TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_TIME, "1@" + TOPIC + "@" + GROUP_NAME));
    }

    @Test
    void testOnGroupDeleted(){
        brokerStatsManager.incGroupGetNums(GROUP_NAME, TOPIC, 1);
        brokerStatsManager.incGroupGetSize(GROUP_NAME, TOPIC, 100);
        brokerStatsManager.incQueueGetNums(GROUP_NAME, TOPIC, QUEUE_ID, 1);
        brokerStatsManager.incQueueGetSize(GROUP_NAME, TOPIC, QUEUE_ID, 100);
        brokerStatsManager.incSendBackNums(GROUP_NAME, TOPIC);
        brokerStatsManager.incGroupGetLatency(GROUP_NAME, TOPIC, 1, 1);
        brokerStatsManager.recordDiskFallBehindTime(GROUP_NAME, TOPIC, 1, 11L);
        brokerStatsManager.recordDiskFallBehindSize(GROUP_NAME, TOPIC, 1, 11L);

        brokerStatsManager.onGroupDeleted(GROUP_NAME);

        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_SIZE, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_NUMS, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_GET_SIZE, TOPIC + "@" + QUEUE_ID + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(QUEUE_GET_NUMS, TOPIC + "@" + QUEUE_ID + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(SNDBCK_PUT_NUMS, TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_LATENCY, "1@" + TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_SIZE, "1@" + TOPIC + "@" + GROUP_NAME));
        Assertions.assertNull(brokerStatsManager.getStatsItem(GROUP_GET_FALL_TIME, "1@" + TOPIC + "@" + GROUP_NAME));
    }
}
