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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


import static org.mockito.Mockito.mock;

public class RouteInfoManagerTest {

    private static RouteInfoManager routeInfoManager;

    @BeforeEach
    public void setup() {
        routeInfoManager = new RouteInfoManager();
        testRegisterBroker();
    }

    @AfterEach
    public void terminate() {
        routeInfoManager.printAllPeriodically();
        routeInfoManager.unregisterBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234);
    }

    @Test
    void testGetAllClusterInfo() {
        byte[] clusterInfo = routeInfoManager.getAllClusterInfo();
        Assertions.assertNotNull(clusterInfo);
    }

    @Test
    void testGetAllTopicList() {
        byte[] topicInfo = routeInfoManager.getAllTopicList();
        Assertions.assertTrue(topicInfo != null);
        Assertions.assertNotNull(topicInfo);
    }

    @Test
    void testRegisterBroker() {
        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setWriteQueueNums(8);
        topicConfig.setTopicName("unit-test");
        topicConfig.setPerm(6);
        topicConfig.setReadQueueNums(8);
        topicConfig.setOrder(false);
        topicConfigConcurrentHashMap.put("unit-test", topicConfig);
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
        Channel channel = mock(Channel.class);
        RegisterBrokerResult registerBrokerResult = routeInfoManager.registerBroker("default-cluster", "127.0.0.1:10911", "default-broker", 1234, "127.0.0.1:1001",
                topicConfigSerializeWrapper, new ArrayList<String>(), channel);
        Assertions.assertNotNull(registerBrokerResult);
    }

    @Test
    void testWipeWritePermOfBrokerByLock() throws Exception {
        List<QueueData> qdList = new ArrayList<>();
        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ | PermName.PERM_WRITE);
        qd.setBrokerName("broker-a");
        qdList.add(qd);
        HashMap<String, List<QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdList);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.wipeWritePermOfBrokerByLock("broker-a");
        Assertions.assertEquals(addTopicCnt,1);
        Assertions.assertEquals(qd.getPerm(),PermName.PERM_READ);

    }

    @Test
    void testPickupTopicRouteData() {
        TopicRouteData result = routeInfoManager.pickupTopicRouteData("unit_test");
        Assertions.assertNull(result);
    }

    @Test
    void testGetSystemTopicList() {
        byte[] topicList = routeInfoManager.getSystemTopicList();
        Assertions.assertNotNull(topicList);
    }

    @Test
    void testGetTopicsByCluster() {
        byte[] topicList = routeInfoManager.getTopicsByCluster("default-cluster");
        Assertions.assertNotNull(topicList);
    }

    @Test
    void testGetUnitTopics() {
        byte[] topicList = routeInfoManager.getUnitTopics();
        Assertions.assertNotNull(topicList);
    }

    @Test
    void testGetHasUnitSubTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubTopicList();
        Assertions.assertNotNull(topicList);
    }

    @Test
    void testGetHasUnitSubUnUnitTopicList() {
        byte[] topicList = routeInfoManager.getHasUnitSubUnUnitTopicList();
        Assertions.assertNotNull(topicList);
    }

    @Test
    void testAddWritePermOfBrokerByLock() throws Exception {
        List<QueueData> qdList = new ArrayList<>();
        QueueData qd = new QueueData();
        qd.setPerm(PermName.PERM_READ);
        qd.setBrokerName("broker-a");
        qdList.add(qd);
        HashMap<String, List<QueueData>> topicQueueTable = new HashMap<>();
        topicQueueTable.put("topic-a", qdList);

        Field filed = RouteInfoManager.class.getDeclaredField("topicQueueTable");
        filed.setAccessible(true);
        filed.set(routeInfoManager, topicQueueTable);

        int addTopicCnt = routeInfoManager.addWritePermOfBrokerByLock("broker-a");
        Assertions.assertEquals(addTopicCnt,1);
        Assertions.assertEquals(qd.getPerm(),PermName.PERM_READ | PermName.PERM_WRITE);

    }
}