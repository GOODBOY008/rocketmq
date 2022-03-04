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

package org.apache.rocketmq.broker.offset;

import java.io.File;
import java.util.Map;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.subscription.LmqSubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.LmqTopicConfigManager;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;


public class LmqConsumerOffsetManagerTest {

    @Spy
    private BrokerController brokerController = new BrokerController(new BrokerConfig(), new NettyServerConfig(),
        new NettyClientConfig(), new MessageStoreConfig());

    @Test
    void testOffsetManage() {
        LmqConsumerOffsetManager lmqConsumerOffsetManager = new LmqConsumerOffsetManager(brokerController);
        LmqTopicConfigManager lmqTopicConfigManager = new LmqTopicConfigManager(brokerController);
        LmqSubscriptionGroupManager lmqSubscriptionGroupManager = new LmqSubscriptionGroupManager(brokerController);

        String lmqTopicName = "%LMQ%1111";
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(lmqTopicName);
        lmqTopicConfigManager.updateTopicConfig(topicConfig);
        TopicConfig topicConfig1 = lmqTopicConfigManager.selectTopicConfig(lmqTopicName);
        Assertions.assertEquals(topicConfig1.getTopicName(),topicConfig.getTopicName());

        String lmqGroupName = "%LMQ%GID_test";
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(lmqGroupName);
        lmqSubscriptionGroupManager.updateSubscriptionGroupConfig(subscriptionGroupConfig);
        SubscriptionGroupConfig subscriptionGroupConfig1 = lmqSubscriptionGroupManager.findSubscriptionGroupConfig(
            lmqGroupName);
        Assertions.assertEquals(subscriptionGroupConfig1.getGroupName(),subscriptionGroupConfig.getGroupName());

        lmqConsumerOffsetManager.commitOffset("127.0.0.1", lmqGroupName, lmqTopicName, 0, 10L);
        Map<Integer, Long> integerLongMap = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName);
        Assertions.assertEquals(integerLongMap.get(0),10L);
        long offset = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName, 0);
        Assertions.assertEquals(offset,10L);

        long offset1 = lmqConsumerOffsetManager.queryOffset(lmqGroupName, lmqTopicName + "test", 0);
        Assertions.assertEquals(offset1,-1L);
    }

    @AfterEach
    public void destroy() {
        UtilAll.deleteFile(new File(new MessageStoreConfig().getStorePathRootDir()));
    }

}