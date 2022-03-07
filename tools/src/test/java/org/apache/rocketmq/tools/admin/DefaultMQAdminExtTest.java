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
package org.apache.rocketmq.tools.admin;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class DefaultMQAdminExtTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;
    private static Properties properties = new Properties();
    private static TopicList topicList = new TopicList();
    private static TopicRouteData topicRouteData = new TopicRouteData();
    private static KVTable kvTable = new KVTable();
    private static ClusterInfo clusterInfo = new ClusterInfo();

    @BeforeEach
    public static void init() throws Exception {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        properties.setProperty("maxMessageSize", "5000000");
        properties.setProperty("flushDelayOffsetInterval", "15000");
        properties.setProperty("serverSocketRcvBufSize", "655350");
        when(mQClientAPIImpl.getBrokerConfig(anyString(), anyLong())).thenReturn(properties);

        Set<String> topicSet = new HashSet<>();
        topicSet.add("topic_one");
        topicSet.add("topic_two");
        topicList.setTopicList(topicSet);
        when(mQClientAPIImpl.getTopicListFromNameServer(anyLong())).thenReturn(topicList);

        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(1234l, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerName("default-broker");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);

        HashMap<String, String> result = new HashMap<>();
        result.put("id", "1234");
        result.put("brokerName", "default-broker");
        kvTable.setTable(result);
        when(mQClientAPIImpl.getBrokerRuntimeInfo(anyString(), anyLong())).thenReturn(kvTable);

        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put("default-broker", brokerData);
        brokerAddrTable.put("broker-test", new BrokerData());
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(new HashMap<String, Set<String>>());
        when(mQClientAPIImpl.getBrokerClusterInfo(anyLong())).thenReturn(clusterInfo);
        when(mQClientAPIImpl.cleanExpiredConsumeQueue(anyString(), anyLong())).thenReturn(true);

        Set<String> clusterList = new HashSet<>();
        clusterList.add("default-cluster-one");
        clusterList.add("default-cluster-two");
        when(mQClientAPIImpl.getClusterList(anyString(), anyLong())).thenReturn(clusterList);

        GroupList groupList = new GroupList();
        HashSet<String> groups = new HashSet<>();
        groups.add("consumer-group-one");
        groups.add("consumer-group-two");
        groupList.setGroupList(groups);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(topicRouteData);
        when(mQClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);

        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptions = new ConcurrentHashMap<>();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setBrokerId(1234);
        subscriptionGroupConfig.setGroupName("Consumer-group-one");
        subscriptions.put("Consumer-group-one", subscriptionGroupConfig);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptions);
        when(mQClientAPIImpl.getAllSubscriptionGroup(anyString(), anyLong())).thenReturn(subscriptionGroupWrapper);

        String topicListConfig = "topicListConfig";
        when(mQClientAPIImpl.getKVConfigValue(anyString(), anyString(), anyLong())).thenReturn(topicListConfig);

        KVTable kvTable = new KVTable();
        HashMap<String, String> kv = new HashMap<>();
        kv.put("broker-name", "broker-one");
        kv.put("cluster-name", "default-cluster");
        kvTable.setTable(kv);
        when(mQClientAPIImpl.getKVListByNamespace(anyString(), anyLong())).thenReturn(kvTable);

        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setConsumeTps(1234);
        MessageQueue messageQueue = new MessageQueue();
        OffsetWrapper offsetWrapper = new OffsetWrapper();
        HashMap<MessageQueue, OffsetWrapper> stats = new HashMap<>();
        stats.put(messageQueue, offsetWrapper);
        consumeStats.setOffsetTable(stats);
        when(mQClientAPIImpl.getConsumeStats(anyString(), anyString(), anyString(), anyLong())).thenReturn(consumeStats);

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerConnection.setMessageModel(MessageModel.CLUSTERING);
        HashSet<Connection> connections = new HashSet<>();
        connections.add(new Connection());
        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<String, SubscriptionData>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);

        ProducerConnection producerConnection = new ProducerConnection();
        Connection connection = new Connection();
        connection.setClientAddr("127.0.0.1:9898");
        connection.setClientId("PID_12345");
        HashSet<Connection> connectionSet = new HashSet<Connection>();
        connectionSet.add(connection);
        producerConnection.setConnectionSet(connectionSet);
        when(mQClientAPIImpl.getProducerConnectionList(anyString(), anyString(), anyLong())).thenReturn(producerConnection);

        when(mQClientAPIImpl.wipeWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(6);
        when(mQClientAPIImpl.addWritePermOfBroker(anyString(), anyString(), anyLong())).thenReturn(7);

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        topicStatsTable.setOffsetTable(new HashMap<MessageQueue, TopicOffset>());

        Map<String, Map<MessageQueue, Long>> consumerStatus = new HashMap<>();
        when(mQClientAPIImpl.invokeBrokerToGetConsumerStatus(anyString(), anyString(), anyString(), anyString(), anyLong())).thenReturn(consumerStatus);

        List<QueueTimeSpan> queueTimeSpanList = new ArrayList<>();
        when(mQClientAPIImpl.queryConsumeTimeSpan(anyString(), anyString(), anyString(), anyLong())).thenReturn(queueTimeSpanList);

        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        consumerRunningInfo.setSubscriptionSet(new TreeSet<SubscriptionData>());
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), anyString(), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        topicConfigSerializeWrapper.setTopicConfigTable(new ConcurrentHashMap<String, TopicConfig>() {
            {
                put("topic_test_examine_topicConfig", new TopicConfig("topic_test_examine_topicConfig"));
            }
        });
        when(mQClientAPIImpl.getAllTopicConfig(anyString(),anyLong())).thenReturn(topicConfigSerializeWrapper);
    }

    @AfterEach
    public static void terminate() throws Exception {
        if (defaultMQAdminExtImpl != null)
            defaultMQAdminExt.shutdown();
    }

    @Test
    void testUpdateBrokerConfig() throws InterruptedException, RemotingConnectException, UnsupportedEncodingException, RemotingTimeoutException, MQBrokerException, RemotingSendRequestException {
        Properties result = defaultMQAdminExt.getBrokerConfig("127.0.0.1:10911");
        Assertions.assertEquals(result.getProperty("maxMessageSize"),"5000000");
        Assertions.assertEquals(result.getProperty("flushDelayOffsetInterval"),"15000");
        Assertions.assertEquals(result.getProperty("serverSocketRcvBufSize"),"655350");
    }

    @Test
    void testFetchAllTopicList() throws RemotingException, MQClientException, InterruptedException {
        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        Assertions.assertEquals(topicList.getTopicList().size(),2);
        Assertions.assertEquals(topicList.getTopicList()).contains("topic_one");
    }

    @Test
    void testFetchBrokerRuntimeStats() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        KVTable brokerStats = defaultMQAdminExt.fetchBrokerRuntimeStats("127.0.0.1:10911");
        Assertions.assertEquals(brokerStats.getTable().get("id"),"1234");
        Assertions.assertEquals(brokerStats.getTable().get("brokerName"),"default-broker");
    }

    @Test
    void testExamineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
        HashMap<String, BrokerData> brokerList = clusterInfo.getBrokerAddrTable();
        Assertions.assertEquals(brokerList.get("default-broker").getBrokerName(),"default-broker");
        Assertions.assertTrue(brokerList.containsKey("broker-test"));

        HashMap<String, Set<String>> clusterMap = new HashMap<>();
        Set<String> brokers = new HashSet<>();
        brokers.add("default-broker");
        brokers.add("broker-test");
        clusterMap.put("default-cluster", brokers);
        ClusterInfo cInfo = mock(ClusterInfo.class);
        when(cInfo.getClusterAddrTable()).thenReturn(clusterMap);
        HashMap<String, Set<String>> clusterAddress = cInfo.getClusterAddrTable();
        Assertions.assertTrue(clusterAddress.containsKey("default-cluster"));
        Assertions.assertEquals(clusterAddress.get("default-cluster").size(),2);
    }

    @Test
    void testExamineConsumeStats() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats("default-consumer-group", "unit-test");
        Assertions.assertEquals(consumeStats.getConsumeTps(),1234);
    }

    @Test
    void testExamineConsumerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo("default-consumer-group");
        Assertions.assertEquals(consumerConnection.getConsumeType(),ConsumeType.CONSUME_PASSIVELY);
        Assertions.assertEquals(consumerConnection.getMessageModel(),MessageModel.CLUSTERING);
    }

    @Test
    void testExamineProducerConnectionInfo() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        ProducerConnection producerConnection = defaultMQAdminExt.examineProducerConnectionInfo("default-producer-group", "unit-test");
        Assertions.assertEquals(producerConnection.getConnectionSet().size(),1);
    }

    @Test
    void testWipeWritePermOfBroker() throws InterruptedException, RemotingCommandException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, RemotingConnectException {
        int result = defaultMQAdminExt.wipeWritePermOfBroker("127.0.0.1:9876", "default-broker");
        Assertions.assertEquals(result,6);
    }

    @Test
    void testAddWritePermOfBroker() throws InterruptedException, RemotingCommandException, RemotingSendRequestException, RemotingTimeoutException, MQClientException, RemotingConnectException {
        int result = defaultMQAdminExt.addWritePermOfBroker("127.0.0.1:9876", "default-broker");
        Assertions.assertEquals(result,7);
    }

    @Test
    void testExamineTopicRouteInfo() throws RemotingException, MQClientException, InterruptedException {
        TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo("UnitTest");
        Assertions.assertEquals(topicRouteData.getBrokerDatas().get(0).getBrokerName(),"default-broker");
        Assertions.assertEquals(topicRouteData.getBrokerDatas().get(0).getCluster(),"default-cluster");
    }

    @Test
    void testGetNameServerAddressList() {
        List<String> result = new ArrayList<>();
        result.add("default-name-one");
        result.add("default-name-two");
        when(mqClientInstance.getMQClientAPIImpl().getNameServerAddressList()).thenReturn(result);
        List<String> nameList = defaultMQAdminExt.getNameServerAddressList();
        Assertions.assertEquals(nameList.get(0),"default-name-one");
        Assertions.assertEquals(nameList.get(1),"default-name-two");
    }

    @Test
    void testPutKVConfig() throws RemotingException, MQClientException, InterruptedException {
        String topicConfig = defaultMQAdminExt.getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest");
        Assertions.assertEquals(topicConfig,"topicListConfig");
        KVTable kvs = defaultMQAdminExt.getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        Assertions.assertEquals(kvs.getTable().get("broker-name"),"broker-one");
        Assertions.assertEquals(kvs.getTable().get("cluster-name"),"default-cluster");
    }

    @Test
    void testQueryTopicConsumeByWho() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        GroupList groupList = defaultMQAdminExt.queryTopicConsumeByWho("UnitTest");
        Assertions.assertTrue(groupList.getGroupList().contains("consumer-group-two"));
    }

    @Test
    void testQueryConsumeTimeSpan() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<QueueTimeSpan> result = defaultMQAdminExt.queryConsumeTimeSpan("unit-test", "default-broker-group");
        Assertions.assertEquals(result.size(),0);
    }

    @Test
    void testCleanExpiredConsumerQueue() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean result = defaultMQAdminExt.cleanExpiredConsumerQueue("default-cluster");
        Assertions.assertFalse(result);
    }

    @Test
    void testCleanExpiredConsumerQueueByAddr() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean clean = defaultMQAdminExt.cleanExpiredConsumerQueueByAddr("127.0.0.1:10911");
        Assertions.assertTrue(clean);
    }

    @Test
    void testCleanUnusedTopic() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        boolean result = defaultMQAdminExt.cleanUnusedTopic("default-cluster");
        Assertions.assertFalse(result);
    }

    @Test
    void testGetConsumerRunningInfo() throws RemotingException, MQClientException, InterruptedException {
        ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo("consumer-group", "cid_123", false);
        Assertions.assertEquals(consumerRunningInfo.getJstack(),"test");
    }

    @Test
    void testMessageTrackDetail() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        MessageExt messageExt = new MessageExt();
        messageExt.setMsgId("msgId");
        messageExt.setTopic("unit-test");
        List<MessageTrack> messageTrackList = defaultMQAdminExt.messageTrackDetail(messageExt);
        Assertions.assertEquals(messageTrackList.size(),2);
    }

    @Test
    void testGetConsumeStatus() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Map<String, Map<MessageQueue, Long>> result = defaultMQAdminExt.getConsumeStatus("unit-test", "default-broker-group", "127.0.0.1:10911");
        Assertions.assertEquals(result.size(),0);
    }

    @Test
    void testGetTopicClusterList() throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Set<String> result = defaultMQAdminExt.getTopicClusterList("unit-test");
        Assertions.assertEquals(result.size(),0);
    }

    @Test
    void testGetClusterList() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        Set<String> clusterlist = defaultMQAdminExt.getClusterList("UnitTest");
        Assertions.assertTrue(clusterlist.contains("default-cluster-one"));
        Assertions.assertTrue(clusterlist.contains("default-cluster-two"));
    }

    @Test
    void testFetchConsumeStatsInBroker() throws InterruptedException, RemotingTimeoutException, MQClientException, RemotingSendRequestException, RemotingConnectException {
        ConsumeStatsList result = new ConsumeStatsList();
        result.setBrokerAddr("127.0.0.1:10911");
        when(mqClientInstance.getMQClientAPIImpl().fetchConsumeStatsInBroker("127.0.0.1:10911", false, 10000)).thenReturn(result);
        ConsumeStatsList consumeStatsList = defaultMQAdminExt.fetchConsumeStatsInBroker("127.0.0.1:10911", false, 10000);
        Assertions.assertEquals(consumeStatsList.getBrokerAddr(),"127.0.0.1:10911");
    }

    @Test
    void testGetAllSubscriptionGroup() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup("127.0.0.1:10911", 10000);
        Assertions.assertEquals(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").getBrokerId(),1234);
        Assertions.assertEquals(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").getGroupName(),"Consumer-group-one");
        Assertions.assertTrue(subscriptionGroupWrapper.getSubscriptionGroupTable().get("Consumer-group-one").isConsumeBroadcastEnable());
    }

    @Test
    void testExamineTopicConfig() throws MQBrokerException, RemotingException, InterruptedException {
        TopicConfig topicConfig = defaultMQAdminExt.examineTopicConfig("127.0.0.1:10911", "topic_test_examine_topicConfig");
        Assertions.assertEquals(topicConfig.getTopicName().equals("topic_test_examine_topicConfig"));
    }
}