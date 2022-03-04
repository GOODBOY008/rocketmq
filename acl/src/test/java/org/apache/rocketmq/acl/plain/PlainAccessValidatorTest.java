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
package org.apache.rocketmq.acl.plain;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.common.AclException;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PlainAccessValidatorTest {

    private PlainAccessValidator plainAccessValidator;
    private AclClientRPCHook aclClient;
    private SessionCredentials sessionCredentials;


    @BeforeEach
    public void init() {
        File file = new File("src/test/resources");
        System.setProperty("rocketmq.home.dir", file.getAbsolutePath());
        plainAccessValidator = new PlainAccessValidator();
        sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ");
        sessionCredentials.setSecretKey("12345678");
        sessionCredentials.setSecurityToken("87654321");
        aclClient = new AclClientRPCHook(sessionCredentials);
    }

    @Test
    void contentTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "127.0.0.1");
            String signature = AclUtils.calSignature(accessResource.getContent(), sessionCredentials.getSecretKey());

            Assertions.assertEquals(accessResource.getSignature(), signature);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }

    }

    @Test
    void validateTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }

    }

    @Test
    void validateSendMessageTest() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateSendMessageV2Test() {
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicC");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(messageRequestHeader));
        aclClient.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateForAdminCommandWithOutAclRPCHook() {
        RemotingCommand consumerOffsetAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        plainAccessValidator.parse(consumerOffsetAdminRequest, "192.168.0.1:9876");

        RemotingCommand subscriptionGroupAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);
        plainAccessValidator.parse(subscriptionGroupAdminRequest, "192.168.0.1:9876");

        RemotingCommand delayOffsetAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);
        plainAccessValidator.parse(delayOffsetAdminRequest, "192.168.0.1:9876");

        RemotingCommand allTopicConfigAdminRequest = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
        plainAccessValidator.parse(allTopicConfigAdminRequest, "192.168.0.1:9876");

    }

    @Test
    void validatePullMessageTest() {
        PullMessageRequestHeader pullMessageRequestHeader = new PullMessageRequestHeader();
        pullMessageRequestHeader.setTopic("topicC");
        pullMessageRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, pullMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateConsumeMessageBackTest() {
        ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader = new ConsumerSendMsgBackRequestHeader();
        consumerSendMsgBackRequestHeader.setOriginTopic("topicC");
        consumerSendMsgBackRequestHeader.setGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateQueryMessageTest() {
        QueryMessageRequestHeader queryMessageRequestHeader = new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("topicC");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, queryMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateQueryMessageByKeyTest() {
        QueryMessageRequestHeader queryMessageRequestHeader = new QueryMessageRequestHeader();
        queryMessageRequestHeader.setTopic("topicC");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, queryMessageRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        remotingCommand.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, "false");
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateHeartBeatTest() {
        HeartbeatData heartbeatData = new HeartbeatData();
        Set<ProducerData> producerDataSet = new HashSet<>();
        Set<ConsumerData> consumerDataSet = new HashSet<>();
        Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("producerGroupA");
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("consumerGroupA");
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic("topicC");
        producerDataSet.add(producerData);
        consumerDataSet.add(consumerData);
        subscriptionDataSet.add(subscriptionData);
        consumerData.setSubscriptionDataSet(subscriptionDataSet);
        heartbeatData.setProducerDataSet(producerDataSet);
        heartbeatData.setConsumerDataSet(consumerDataSet);
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        remotingCommand.setBody(heartbeatData.encode());
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encode();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateUnRegisterClientTest() {
        UnregisterClientRequestHeader unregisterClientRequestHeader = new UnregisterClientRequestHeader();
        unregisterClientRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, unregisterClientRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateGetConsumerListByGroupTest() {
        GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader = new GetConsumerListByGroupRequestHeader();
        getConsumerListByGroupRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, getConsumerListByGroupRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateUpdateConsumerOffSetTest() {
        UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader = new UpdateConsumerOffsetRequestHeader();
        updateConsumerOffsetRequestHeader.setConsumerGroup("consumerGroupA");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, updateConsumerOffsetRequestHeader);
        aclClient.doBeforeRequest("", remotingCommand);
        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.0.1:9876");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void validateNullAccessKeyTest() {
        Assertions.assertThrowsExactly(AclException.class,()-> {
        SessionCredentials sessionCredentials = new SessionCredentials();
        sessionCredentials.setAccessKey("RocketMQ1");
        sessionCredentials.setSecretKey("1234");
        AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(sessionCredentials);
        SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
        messageRequestHeader.setTopic("topicB");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
        aclClientRPCHook.doBeforeRequest("", remotingCommand);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1");
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }});
    }

    @Test
    void validateErrorSecretKeyTest() {
        Assertions.assertThrowsExactly(AclException.class,()-> {
            SessionCredentials sessionCredentials = new SessionCredentials();
            sessionCredentials.setAccessKey("RocketMQ");
            sessionCredentials.setSecretKey("1234");
            AclClientRPCHook aclClientRPCHook = new AclClientRPCHook(sessionCredentials);
            SendMessageRequestHeader messageRequestHeader = new SendMessageRequestHeader();
            messageRequestHeader.setTopic("topicB");
            RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, messageRequestHeader);
            aclClientRPCHook.doBeforeRequest("", remotingCommand);

            ByteBuffer buf = remotingCommand.encodeHeader();
            buf.getInt();
            buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
            buf.position(0);
            try {
                PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), "192.168.1.1");
                plainAccessValidator.validate(accessResource);
            } catch (RemotingCommandException e) {
                e.printStackTrace();

                Assertions.fail("Should not throw IOException");
            }
        });
    }

    @Test
    void validateGetAllTopicConfigTest() {
        String whiteRemoteAddress = "192.168.0.1";
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        ByteBuffer buf = remotingCommand.encodeHeader();
        buf.getInt();
        buf = ByteBuffer.allocate(buf.limit() - buf.position()).put(buf);
        buf.position(0);
        try {
            PlainAccessResource accessResource = (PlainAccessResource) plainAccessValidator.parse(RemotingCommand.decode(buf), whiteRemoteAddress);
            plainAccessValidator.validate(accessResource);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void addAccessAclYamlConfigTest() throws InterruptedException {
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("rocketmq3");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setWhiteRemoteAddress("192.168.0.*");
        plainAccessConfig.setDefaultGroupPerm("PUB");
        plainAccessConfig.setDefaultTopicPerm("SUB");
        List<String> topicPerms = new ArrayList<String>();
        topicPerms.add("topicC=PUB|SUB");
        topicPerms.add("topicB=PUB");
        plainAccessConfig.setTopicPerms(topicPerms);
        List<String> groupPerms = new ArrayList<String>();
        groupPerms.add("groupB=PUB|SUB");
        groupPerms.add("groupC=DENY");
        plainAccessConfig.setGroupPerms(groupPerms);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.updateAccessConfig(plainAccessConfig);
        Thread.sleep(10000);

        Map<String, Object> verifyMap = new HashMap<>();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig1 : plainAccessConfigs) {
            if (plainAccessConfig1.getAccessKey().equals(plainAccessConfig.getAccessKey())) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig1.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig1.getDefaultTopicPerm());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig1.getDefaultGroupPerm());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig1.isAdmin());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig1.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig1.getTopicPerms());
                verifyMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig1.getGroupPerms());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "1234567890");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_TOPIC_PERM), "SUB");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_GROUP_PERM), "PUB");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), false);
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "192.168.0.*");
        Assertions.assertEquals(((List) verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).size(), 2);
        Assertions.assertEquals(((List) verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).size(), 2);

        String aclFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> readableMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assertions.assertEquals(1, dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));

        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void getAccessAclYamlConfigTest() {
        String accessKey = "rocketmq2";
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        Map<String, Object> verifyMap = new HashMap<>();
        for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
            if (plainAccessConfig.getAccessKey().equals(accessKey)) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "12345678");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), true);
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "192.168.1.*");

        String aclFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl.yml";
        Map<String, DataVersion> dataVersionMap = plainAccessValidator.getAllAclConfigVersion();
        DataVersion dataVersion = dataVersionMap.get(aclFileName);
        Assertions.assertEquals(0, dataVersion.getCounter().get());
    }

    @Test
    void updateAccessAclYamlConfigTest() throws InterruptedException{
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("rocketmq3");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setWhiteRemoteAddress("192.168.0.*");
        plainAccessConfig.setDefaultGroupPerm("PUB");
        plainAccessConfig.setDefaultTopicPerm("SUB");
        List<String> topicPerms = new ArrayList<String>();
        topicPerms.add("topicC=PUB|SUB");
        topicPerms.add("topicB=PUB");
        plainAccessConfig.setTopicPerms(topicPerms);
        List<String> groupPerms = new ArrayList<String>();
        groupPerms.add("groupB=PUB|SUB");
        groupPerms.add("groupC=DENY");
        plainAccessConfig.setGroupPerms(groupPerms);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Thread.sleep(10000);

        PlainAccessConfig plainAccessConfig1 = new PlainAccessConfig();
        plainAccessConfig1.setAccessKey("rocketmq3");
        plainAccessConfig1.setSecretKey("1234567891");
        plainAccessConfig1.setWhiteRemoteAddress("192.168.0.*");
        plainAccessConfig1.setDefaultGroupPerm("PUB");
        plainAccessConfig1.setDefaultTopicPerm("SUB");
        List<String> topicPerms1 = new ArrayList<String>();
        topicPerms1.add("topicC=PUB|SUB");
        topicPerms1.add("topicB=PUB");
        plainAccessConfig1.setTopicPerms(topicPerms1);
        List<String> groupPerms1 = new ArrayList<String>();
        groupPerms1.add("groupB=PUB|SUB");
        groupPerms1.add("groupC=DENY");
        plainAccessConfig1.setGroupPerms(groupPerms1);

        plainAccessValidator.updateAccessConfig(plainAccessConfig1);

        Thread.sleep(10000);

        Map<String, Object> verifyMap = new HashMap<>();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig2 : plainAccessConfigs) {
            if (plainAccessConfig2.getAccessKey().equals(plainAccessConfig1.getAccessKey())) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig2.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig2.getDefaultTopicPerm());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig2.getDefaultGroupPerm());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig2.isAdmin());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig2.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig2.getTopicPerms());
                verifyMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig2.getGroupPerms());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "1234567891");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_TOPIC_PERM), "SUB");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_DEFAULT_GROUP_PERM), "PUB");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), false);
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "192.168.0.*");
        Assertions.assertEquals(((List) verifyMap.get(AclConstants.CONFIG_TOPIC_PERMS)).size(), 2);
        Assertions.assertEquals(((List) verifyMap.get(AclConstants.CONFIG_GROUP_PERMS)).size(), 2);

        String aclFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> readableMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assertions.assertEquals(2, dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));

        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void deleteAccessAclYamlConfigTest() throws InterruptedException {
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("rocketmq3");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setWhiteRemoteAddress("192.168.0.*");
        plainAccessConfig.setDefaultGroupPerm("PUB");
        plainAccessConfig.setDefaultTopicPerm("SUB");
        List<String> topicPerms = new ArrayList<String>();
        topicPerms.add("topicC=PUB|SUB");
        topicPerms.add("topicB=PUB");
        plainAccessConfig.setTopicPerms(topicPerms);
        List<String> groupPerms = new ArrayList<String>();
        groupPerms.add("groupB=PUB|SUB");
        groupPerms.add("groupC=DENY");
        plainAccessConfig.setGroupPerms(groupPerms);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        String accessKey = "rocketmq3";
        plainAccessValidator.deleteAccessConfig(accessKey);
        Thread.sleep(10000);

        Map<String, Object> verifyMap = new HashMap<>();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig1 : plainAccessConfigs) {
            if (plainAccessConfig1.getAccessKey().equals(accessKey)) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig1.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig1.getDefaultTopicPerm());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig1.getDefaultGroupPerm());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig1.isAdmin());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig1.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig1.getTopicPerms());
                verifyMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig1.getGroupPerms());
            }
        }

        Assertions.assertEquals(verifyMap.size(), 0);

        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void updateGlobalWhiteRemoteAddressesTest() throws InterruptedException {
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        List<String> globalWhiteAddrsList = new ArrayList<>();
        globalWhiteAddrsList.add("192.168.1.*");

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        Assertions.assertEquals(plainAccessValidator.updateGlobalWhiteAddrsConfig(globalWhiteAddrsList), true);

        String aclFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> readableMap = AclUtils.getYamlDataObject(aclFileName, Map.class);
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assertions.assertEquals(1, dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void addYamlConfigTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        transport.delete();
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: watchrocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        Map<String, Object> verifyMap = new HashMap<>();
        for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
            if (plainAccessConfig.getAccessKey().equals("watchrocketmqx")) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "12345678");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "127.0.0.1");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), true);

        Map<String, DataVersion> dataVersionMap = plainAccessValidator.getAllAclConfigVersion();
        DataVersion dataVersion = dataVersionMap.get(fileName);
        Assertions.assertEquals(0, dataVersion.getCounter().get());

        transport.delete();
    }

    @Test
    void updateAccessAnotherAclYamlConfigTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        transport.delete();
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: watchrocketmqy\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.write("- accessKey: watchrocketmqx\r\n");
        writer.write("  secretKey: 123456781\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("watchrocketmqy");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setWhiteRemoteAddress("127.0.0.1");
        plainAccessConfig.setAdmin(false);

        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Thread.sleep(1000);

        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        Map<String, Object> verifyMap = new HashMap<>();
        for (PlainAccessConfig plainAccessConfig1 : plainAccessConfigs) {
            if (plainAccessConfig1.getAccessKey().equals("watchrocketmqy")) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig1.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig1.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig1.isAdmin());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "1234567890");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "127.0.0.1");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), false);

        Map<String, DataVersion> dataVersionMap = plainAccessValidator.getAllAclConfigVersion();
        DataVersion dataVersion = dataVersionMap.get(fileName);
        Assertions.assertEquals(1, dataVersion.getCounter().get());

        transport.delete();

    }

    @Test
    void createAndUpdateAccessAclNullSkExceptionTest() {
        Assertions.assertThrowsExactly(AclException.class,()-> {
            String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl.yml";
            Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

            PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
            plainAccessConfig.setAccessKey("RocketMQ33");
            // secret key is null

            PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
            plainAccessValidator.updateAccessConfig(plainAccessConfig);

            AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
        });
    }

    @Test
    void addAccessDefaultAclYamlConfigTest() throws InterruptedException {
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        plainAccessConfig.setAccessKey("watchrocketmqh");
        plainAccessConfig.setSecretKey("1234567890");
        plainAccessConfig.setWhiteRemoteAddress("127.0.0.1");
        plainAccessConfig.setAdmin(false);

        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        Thread.sleep(10000);

        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        Map<String, Object> verifyMap = new HashMap<>();
        for (PlainAccessConfig plainAccessConfig1 : plainAccessConfigs) {
            if (plainAccessConfig1.getAccessKey().equals("watchrocketmqh")) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig1.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig1.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig1.isAdmin());
            }
        }

        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_SECRET_KEY), "1234567890");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_WHITE_ADDR), "127.0.0.1");
        Assertions.assertEquals(verifyMap.get(AclConstants.CONFIG_ADMIN_ROLE), false);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<Map<String, Object>> dataVersions = (List<Map<String, Object>>) readableMap.get(AclConstants.CONFIG_DATA_VERSION);
        Assertions.assertEquals(1, dataVersions.get(0).get(AclConstants.CONFIG_COUNTER));

        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void deleteAccessAnotherAclYamlConfigTest() throws IOException, InterruptedException {
        String fileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/acl/plain_acl_test.yml";
        File transport = new File(fileName);
        transport.delete();
        transport.createNewFile();
        FileWriter writer = new FileWriter(transport);
        writer.write("accounts:\r\n");
        writer.write("- accessKey: watchrocketmqx\r\n");
        writer.write("  secretKey: 12345678\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: true\r\n");
        writer.write("- accessKey: watchrocketmqy\r\n");
        writer.write("  secretKey: 1234567890\r\n");
        writer.write("  whiteRemoteAddress: 127.0.0.1\r\n");
        writer.write("  admin: false\r\n");
        writer.flush();
        writer.close();

        Thread.sleep(1000);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        plainAccessValidator.deleteAccessConfig("watchrocketmqx");
        Thread.sleep(10000);

        Map<String, Object> verifyMap = new HashMap<>();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        List<PlainAccessConfig> plainAccessConfigs = aclConfig.getPlainAccessConfigs();
        for (PlainAccessConfig plainAccessConfig : plainAccessConfigs) {
            if (plainAccessConfig.getAccessKey().equals("watchrocketmqx")) {
                verifyMap.put(AclConstants.CONFIG_SECRET_KEY, plainAccessConfig.getSecretKey());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_TOPIC_PERM, plainAccessConfig.getDefaultTopicPerm());
                verifyMap.put(AclConstants.CONFIG_DEFAULT_GROUP_PERM, plainAccessConfig.getDefaultGroupPerm());
                verifyMap.put(AclConstants.CONFIG_ADMIN_ROLE, plainAccessConfig.isAdmin());
                verifyMap.put(AclConstants.CONFIG_WHITE_ADDR, plainAccessConfig.getWhiteRemoteAddress());
                verifyMap.put(AclConstants.CONFIG_TOPIC_PERMS, plainAccessConfig.getTopicPerms());
                verifyMap.put(AclConstants.CONFIG_GROUP_PERMS, plainAccessConfig.getGroupPerms());
            }
        }

        Assertions.assertEquals(verifyMap.size(), 0);

        transport.delete();
    }

    @Test
    void getAllAclConfigTest() {
        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        AclConfig aclConfig = plainAccessValidator.getAllAclConfig();
        Assertions.assertEquals(aclConfig.getGlobalWhiteAddrs().size(), 4);
        Assertions.assertEquals(aclConfig.getPlainAccessConfigs().size(), 2);
    }

    @Test
    void updateAccessConfigEmptyPermListTest() {
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        String accessKey = "updateAccessConfigEmptyPerm";
        plainAccessConfig.setAccessKey(accessKey);
        plainAccessConfig.setSecretKey("123456789111");
        plainAccessConfig.setTopicPerms(Collections.singletonList("topicB=PUB"));
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        plainAccessConfig.setTopicPerms(new ArrayList<>());
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        List<PlainAccessConfig> plainAccessConfigs = plainAccessValidator.getAllAclConfig().getPlainAccessConfigs();
        for (int i = 0; i < plainAccessConfigs.size(); i++) {
            PlainAccessConfig plainAccessConfig1 = plainAccessConfigs.get(i);
            if (plainAccessConfig1.getAccessKey() == accessKey) {
                Assertions.assertEquals(0, plainAccessConfig1.getTopicPerms().size());
            }
        }

        plainAccessValidator.deleteAccessConfig(accessKey);
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }

    @Test
    void updateAccessConfigEmptyWhiteRemoteAddressTest() {
        String targetFileName = System.getProperty("rocketmq.home.dir") + File.separator + "conf/plain_acl.yml";
        Map<String, Object> backUpAclConfigMap = AclUtils.getYamlDataObject(targetFileName, Map.class);

        PlainAccessValidator plainAccessValidator = new PlainAccessValidator();
        PlainAccessConfig plainAccessConfig = new PlainAccessConfig();
        String accessKey = "updateAccessConfigEmptyWhiteRemoteAddress";
        plainAccessConfig.setAccessKey(accessKey);
        plainAccessConfig.setSecretKey("123456789111");
        plainAccessConfig.setWhiteRemoteAddress("127.0.0.1");
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        plainAccessConfig.setWhiteRemoteAddress("");
        plainAccessValidator.updateAccessConfig(plainAccessConfig);

        List<PlainAccessConfig> plainAccessConfigs = plainAccessValidator.getAllAclConfig().getPlainAccessConfigs();
        for (int i = 0; i < plainAccessConfigs.size(); i++) {
            PlainAccessConfig plainAccessConfig1 = plainAccessConfigs.get(i);
            if (plainAccessConfig1.getAccessKey() == accessKey) {
                Assertions.assertEquals("", plainAccessConfig1.getWhiteRemoteAddress());
            }
        }

        plainAccessValidator.deleteAccessConfig(accessKey);
        AclUtils.writeDataObject(targetFileName, backUpAclConfigMap);
    }
}
