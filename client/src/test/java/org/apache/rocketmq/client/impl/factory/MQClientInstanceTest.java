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
package org.apache.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class MQClientInstanceTest {
    private MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

    @BeforeEach
    public void init() throws Exception {
        FieldUtils.writeDeclaredField(mqClientInstance, "brokerAddrTable", brokerAddrTable, true);
    }

    @Test
    void testTopicRouteData2TopicPublishInfo() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);

        Assertions.assertFalse(topicPublishInfo.isHaveTopicRouterInfo());
        Assertions.assertEquals(topicPublishInfo.getMessageQueueList().size(),4);
    }

    @Test
    void testFindBrokerAddressInSubscribe() {
        // dledger normal case
        String brokerName = "BrokerA";
        HashMap<Long, String> addrMap = new HashMap<Long, String>();
        addrMap.put(0L, "127.0.0.1:10911");
        addrMap.put(1L, "127.0.0.1:10912");
        addrMap.put(2L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMap);
        long brokerId = 1;
        FindBrokerResult brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        Assertions.assertNotNull(brokerResult);
        Assertions.assertEquals(brokerResult.getBrokerAddr(),"127.0.0.1:10912");
        Assertions.assertTrue(brokerResult.isSlave());

        // dledger case, when node n0 was voted as the leader
        brokerName = "BrokerB";
        HashMap<Long, String> addrMapNew = new HashMap<Long, String>();
        addrMapNew.put(0L, "127.0.0.1:10911");
        addrMapNew.put(2L, "127.0.0.1:10912");
        addrMapNew.put(3L, "127.0.0.1:10913");
        brokerAddrTable.put(brokerName, addrMapNew);
        brokerResult = mqClientInstance.findBrokerAddressInSubscribe(brokerName, brokerId, false);
        Assertions.assertNotNull(brokerResult);
        Assertions.assertEquals(brokerResult.getBrokerAddr(),"127.0.0.1:10912");
        Assertions.assertTrue(brokerResult.isSlave());
    }

    @Test
    void testRegisterProducer() {
        boolean flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        Assertions.assertTrue(flag);

        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        Assertions.assertFalse(flag);

        mqClientInstance.unregisterProducer(group);
        flag = mqClientInstance.registerProducer(group, mock(DefaultMQProducerImpl.class));
        Assertions.assertTrue(flag);
    }

    @Test
    void testRegisterConsumer() throws RemotingException, InterruptedException, MQBrokerException {
        boolean flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        Assertions.assertTrue(flag);

        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        Assertions.assertFalse(flag);

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        Assertions.assertTrue(flag);
    }


    @Test
    void testConsumerRunningInfoWhenConsumersIsEmptyOrNot() throws RemotingException, InterruptedException, MQBrokerException {
        MQConsumerInner mockConsumerInner = mock(MQConsumerInner.class);
        ConsumerRunningInfo mockConsumerRunningInfo = mock(ConsumerRunningInfo.class);
        when(mockConsumerInner.consumerRunningInfo()).thenReturn(mockConsumerRunningInfo);
        when(mockConsumerInner.consumeType()).thenReturn(ConsumeType.CONSUME_PASSIVELY);
        Properties properties = new Properties();
        when(mockConsumerRunningInfo.getProperties()).thenReturn(properties);
        mqClientInstance.unregisterConsumer(group);

        ConsumerRunningInfo runningInfo = mqClientInstance.consumerRunningInfo(group);
        Assertions.assertNull(runningInfo);
        boolean flag = mqClientInstance.registerConsumer(group, mockConsumerInner);
        Assertions.assertTrue(flag);

        runningInfo = mqClientInstance.consumerRunningInfo(group);
        Assertions.assertNotNull(runningInfo);
        Assertions.assertEquals(mockConsumerInner.consumerRunningInfo().getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE));

        mqClientInstance.unregisterConsumer(group);
        flag = mqClientInstance.registerConsumer(group, mock(MQConsumerInner.class));
        Assertions.assertTrue(flag);
    }

    @Test
    void testRegisterAdminExt() {
        boolean flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        Assertions.assertTrue(flag);

        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        Assertions.assertFalse(flag);

        mqClientInstance.unregisterAdminExt(group);
        flag = mqClientInstance.registerAdminExt(group, mock(MQAdminExtInner.class));
        Assertions.assertTrue(flag);
    }
}