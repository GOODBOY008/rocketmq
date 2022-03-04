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
package org.apache.rocketmq.common.admin;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;


public class TopicStatsTableTest {

    private volatile TopicStatsTable topicStatsTable;

    private static final String TEST_TOPIC = "test_topic";

    private static final String TEST_BROKER = "test_broker";

    private static final int QUEUE_ID = 1;

    private static final long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private static final long MAX_OFFSET = CURRENT_TIME_MILLIS + 100;

    private static final long MIN_OFFSET = CURRENT_TIME_MILLIS - 100;

    @BeforeEach
    public void buildTopicStatsTable() {
        HashMap<MessageQueue, TopicOffset> offsetTableMap = new HashMap<MessageQueue, TopicOffset>();

        MessageQueue messageQueue = new MessageQueue(TEST_TOPIC, TEST_BROKER, QUEUE_ID);

        TopicOffset topicOffset = new TopicOffset();
        topicOffset.setLastUpdateTimestamp(CURRENT_TIME_MILLIS);
        topicOffset.setMinOffset(MIN_OFFSET);
        topicOffset.setMaxOffset(MAX_OFFSET);

        offsetTableMap.put(messageQueue, topicOffset);

        topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(offsetTableMap);
    }

    @Test
    void testGetOffsetTable() throws Exception {
        validateTopicStatsTable(topicStatsTable);
    }

    @Test
    void testFromJson() throws Exception {
        String json = RemotingSerializable.toJson(topicStatsTable, true);
        TopicStatsTable fromJson = RemotingSerializable.fromJson(json, TopicStatsTable.class);

        validateTopicStatsTable(fromJson);
    }

    private static void validateTopicStatsTable(TopicStatsTable topicStatsTable) throws Exception {
        Map.Entry<MessageQueue, TopicOffset> savedTopicStatsTableMap = topicStatsTable.getOffsetTable().entrySet().iterator().next();
        MessageQueue savedMessageQueue = savedTopicStatsTableMap.getKey();
        TopicOffset savedTopicOffset = savedTopicStatsTableMap.getValue();

        Assertions.assertTrue(savedMessageQueue.getTopic().equals(TEST_TOPIC));
        Assertions.assertTrue(savedMessageQueue.getBrokerName().equals(TEST_BROKER));
        Assertions.assertTrue(savedMessageQueue.getQueueId() == QUEUE_ID);

        Assertions.assertTrue(savedTopicOffset.getLastUpdateTimestamp() == CURRENT_TIME_MILLIS);
        Assertions.assertTrue(savedTopicOffset.getMaxOffset() == MAX_OFFSET);
        Assertions.assertTrue(savedTopicOffset.getMinOffset() == MIN_OFFSET);
    }

}
