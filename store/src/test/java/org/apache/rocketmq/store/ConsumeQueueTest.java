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

package org.apache.rocketmq.store;

import java.io.File;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class ConsumeQueueTest {

    private static final String msg = "Once, there was a chance for me!";
    private static final byte[] msgBody = msg.getBytes();

    private static final String topic = "abc";
    private static final int queueId = 0;
    private static final String storePath = "." + File.separator + "unit_test_store";
    private static final int commitLogFileSize = 1024 * 8;
    private static final int cqFileSize = 10 * 20;
    private static final int cqExtFileSize = 10 * (ConsumeQueueExt.CqExtUnit.MIN_EXT_UNIT_SIZE + 64);

    private static SocketAddress BornHost;

    private static SocketAddress StoreHost;

    static {
        try {
            StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageExtBrokerInner buildIPv6HostMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(msgBody);
        msg.setMsgId("24084004018081003FAA1DDE2B3F898A00002A9F0000000000000CA0");
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId);
        msg.setSysFlag(0);
        msg.setBornHostV6Flag();
        msg.setStoreHostAddressV6Flag();
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setBornHost(new InetSocketAddress("1050:0000:0000:0000:0005:0600:300c:326b", 123));
        msg.setStoreHost(new InetSocketAddress("::1", 124));
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    public MessageStoreConfig buildStoreConfig(int commitLogFileSize, int cqFileSize,
        boolean enableCqExt, int cqExtFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(commitLogFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(cqFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueueExt(cqExtFileSize);
        messageStoreConfig.setMessageIndexEnable(false);
        messageStoreConfig.setEnableConsumeQueueExt(enableCqExt);

        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + "commitlog");

        return messageStoreConfig;
    }

    protected DefaultMessageStore gen() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
            commitLogFileSize, cqFileSize, true, cqExtFileSize
        );

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat()),
            new MessageArrivingListener() {
                @Override
                public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                    long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                }
            }
            , brokerConfig);

        Assertions.assertTrue(master.load());

        master.start();

        return master;
    }

    protected DefaultMessageStore genForMultiQueue() throws Exception {
        MessageStoreConfig messageStoreConfig = buildStoreConfig(
            commitLogFileSize, cqFileSize, true, cqExtFileSize
        );

        messageStoreConfig.setEnableLmq(true);
        messageStoreConfig.setEnableMultiDispatch(true);

        BrokerConfig brokerConfig = new BrokerConfig();

        DefaultMessageStore master = new DefaultMessageStore(
            messageStoreConfig,
            new BrokerStatsManager(brokerConfig.getBrokerClusterName(), brokerConfig.isEnableDetailStat()),
            new MessageArrivingListener() {
                @Override
                public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                    long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
                }
            }
            , brokerConfig);

        Assertions.assertTrue(master.load());

        master.start();

        return master;
    }

    protected void putMsg(DefaultMessageStore master) throws Exception {
        long totalMsgs = 200;

        for (long i = 0; i < totalMsgs; i++) {
            if (i < totalMsgs / 2) {
                master.putMessage(buildMessage());
            } else {
                master.putMessage(buildIPv6HostMessage());
            }
        }
    }

    protected void putMsgMultiQueue(DefaultMessageStore master) throws Exception {
        for (long i = 0; i < 1; i++) {
            master.putMessage(buildMessageMultiQueue());
        }
    }

    private MessageExtBrokerInner buildMessageMultiQueue() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(msgBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(queueId);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(StoreHost);
        msg.setBornHost(BornHost);
        for (int i = 0; i < 1; i++) {
            msg.putUserProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%123,%LMQ%456");
            msg.putUserProperty(String.valueOf(i), "imagoodperson" + i);
        }
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

        return msg;
    }

    protected void deleteDirectory(String rootPath) {
        File file = new File(rootPath);
        deleteFile(file);
    }

    protected void deleteFile(File file) {
        File[] subFiles = file.listFiles();
        if (subFiles != null) {
            for (File sub : subFiles) {
                deleteFile(sub);
            }
        }

        file.delete();
    }

    @Test
    void testPutMessagePositionInfo_buildCQRepeatedly() throws Exception {
        DefaultMessageStore messageStore = null;
        try {

            messageStore = gen();

            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsg(messageStore);
            }
            Thread.sleep(5);

            ConsumeQueue cq = messageStore.getConsumeQueueTable().get(topic).get(queueId);
            Method method = cq.getClass().getDeclaredMethod("putMessagePositionInfo", long.class, int.class, long.class, long.class);

            Assertions.assertNotNull(method);

            method.setAccessible(true);

            SelectMappedBufferResult result = messageStore.getCommitLog().getData(0);
            Assertions.assertTrue(result != null);

            DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);

            Assertions.assertNotNull(cq);

            Object dispatchResult = method.invoke(cq, dispatchRequest.getCommitLogOffset(),
                dispatchRequest.getMsgSize(), dispatchRequest.getTagsCode(), dispatchRequest.getConsumeQueueOffset());

            Assertions.assertTrue(Boolean.parseBoolean(dispatchResult.toString()));

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(storePath);
        }

    }

    @Test
    void testPutMessagePositionInfoWrapper_MultiQueue() throws Exception {
        DefaultMessageStore messageStore = null;
        try {
            messageStore = genForMultiQueue();


            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsgMultiQueue(messageStore);
            }
            Thread.sleep(5);

            ConsumeQueue cq = messageStore.getConsumeQueueTable().get(topic).get(queueId);
            Method method = cq.getClass().getDeclaredMethod("putMessagePositionInfoWrapper", DispatchRequest.class, boolean.class);

            Assertions.assertNotNull(method);

            method.setAccessible(true);

            SelectMappedBufferResult result = messageStore.getCommitLog().getData(0);
            Assertions.assertTrue(result != null);

            DispatchRequest dispatchRequest = messageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer(), false, false);

            Assertions.assertNotNull(cq);

            Object dispatchResult = method.invoke(cq,  dispatchRequest, true);

            ConsumeQueue lmqCq1 = messageStore.getConsumeQueueTable().get("%LMQ%123").get(0);

            ConsumeQueue lmqCq2 = messageStore.getConsumeQueueTable().get("%LMQ%456").get(0);

            Assertions.assertNotNull(lmqCq1);

            Assertions.assertNotNull(lmqCq2);

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(storePath);
        }

    }

    @Test
    void testPutMessagePositionInfoMultiQueue() throws Exception {
        DefaultMessageStore messageStore = null;
        try {

            messageStore = genForMultiQueue();

            int totalMessages = 10;

            for (int i = 0; i < totalMessages; i++) {
                putMsgMultiQueue(messageStore);
            }
            Thread.sleep(5);

            ConsumeQueue cq = messageStore.getConsumeQueueTable().get(topic).get(queueId);

            ConsumeQueue lmqCq1 = messageStore.getConsumeQueueTable().get("%LMQ%123").get(0);

            ConsumeQueue lmqCq2 = messageStore.getConsumeQueueTable().get("%LMQ%456").get(0);

            Assertions.assertNotNull(cq);

            Assertions.assertNotNull(lmqCq1);

            Assertions.assertNotNull(lmqCq2);

        } finally {
            if (messageStore != null) {
                messageStore.shutdown();
                messageStore.destroy();
            }
            deleteDirectory(storePath);
        }
    }

    @Test
    void testConsumeQueueWithExtendData() {
        DefaultMessageStore master = null;
        try {
            master = gen();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(Boolean.FALSE);
        }

        master.getDispatcherList().addFirst(new CommitLogDispatcher() {

            @Override
            public void dispatch(DispatchRequest request) {
                runCount++;
            }

            private int runCount = 0;
        });

        try {
            try {
                putMsg(master);
                Thread.sleep(3000L);//wait ConsumeQueue create success.
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.assertTrue(Boolean.FALSE);
            }

            ConsumeQueue cq = master.getConsumeQueueTable().get(topic).get(queueId);

            Assertions.assertNotNull(cq);

            long index = 0;

            while (index < cq.getMaxOffsetInQueue()) {
                SelectMappedBufferResult bufferResult = cq.getIndexBuffer(index);

                Assertions.assertNotNull(bufferResult);

                ByteBuffer buffer = bufferResult.getByteBuffer();

                Assertions.assertNotNull(buffer);
                try {
                    ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                    for (int i = 0; i < bufferResult.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long phyOffset = buffer.getLong();
                        int size = buffer.getInt();
                        long tagsCode = buffer.getLong();

                        Assertions.assertEquals(phyOffset).isGreaterThanOrEqualTo(0);
                        Assertions.assertEquals(size).isGreaterThan(0);
                        Assertions.assertEquals(tagsCode).isLessThan(0);

                        boolean ret = cq.getExt(tagsCode, cqExtUnit);

                        Assertions.assertTrue(ret);
                        Assertions.assertNotNull(cqExtUnit);
                        Assertions.assertEquals(cqExtUnit.getSize()).isGreaterThan((short) 0);
                        Assertions.assertEquals(cqExtUnit.getMsgStoreTime()).isGreaterThan(0);
                        Assertions.assertEquals(cqExtUnit.getTagsCode()).isGreaterThan(0);
                    }

                } finally {
                    bufferResult.release();
                }

                index += cqFileSize / ConsumeQueue.CQ_STORE_UNIT_SIZE;
            }
        } finally {
            master.shutdown();
            master.destroy();
            UtilAll.deleteFile(new File(storePath));
        }
    }
}
