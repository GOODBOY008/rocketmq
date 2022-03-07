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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.client.producer.exception.msg;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.factory.MessageFactory;
import org.apache.rocketmq.test.factory.ProducerFactory;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class MessageExceptionIT extends BaseConf {
    private static DefaultMQProducer producer = null;
    private static String topic = null;

    @BeforeEach
    public void setUp() {
        producer = ProducerFactory.getRMQProducer(nsAddr);
        topic = initTopic();
    }

    @AfterEach
    public void tearDown() {
        producer.shutdown();
    }

    @Test
    void testProducerSmoke() {
        Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
        }

        Assertions.assertNotEquals(sendResult,null);
        Assertions.assertEquals(sendResult.getSendStatus(),SendStatus.SEND_OK);
    }

    @Test
    void testSynSendNullMessage() throws Exception {
        Assertions.assertThrowsExactly(java.lang.NullPointerException.class,()-> producer.send((Message) null));
    }

    @Test
    void testSynSendNullBodyMessage() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
            msg.setBody(null);
            producer.send(msg);
        });
    }

    @Test
    void testSynSendZeroSizeBodyMessage() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
            msg.setBody(new byte[0]);
            producer.send(msg);
        });
    }

    @Test
    void testSynSendOutOfSizeBodyMessage() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message(topic, RandomUtils.getStringByUUID().getBytes());
            msg.setBody(new byte[1024 * 1024 * 4 + 1]);
            producer.send(msg);
        });
    }

    @Test
    void testSynSendNullTopicMessage() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message(null, RandomUtils.getStringByUUID().getBytes());
            producer.send(msg);
        });
    }

    @Test
    void testSynSendBlankTopicMessage() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message("", RandomUtils.getStringByUUID().getBytes());
            producer.send(msg);
        });
    }

    @Test
    void testSend128kMsg() throws Exception {
        Assertions.assertThrowsExactly(org.apache.rocketmq.client.exception.MQClientException.class,()->{
            Message msg = new Message(topic,
                    RandomUtils.getStringWithNumber(1024 * 1024 * 4 + 1).getBytes());
            producer.send(msg);
        });
    }

    @Test
    void testSendLess128kMsg() {
        Message msg = new Message(topic, RandomUtils.getStringWithNumber(128 * 1024).getBytes());
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
        }
        Assertions.assertEquals(sendResult.getSendStatus(),SendStatus.SEND_OK);
    }

    @Test
    void testSendMsgWithUserProperty() {
        Message msg = MessageFactory.getRandomMessage(topic);
        msg.putUserProperty("key", RandomUtils.getCheseWord(10 * 1024));
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
        }
        Assertions.assertEquals(sendResult.getSendStatus(),SendStatus.SEND_OK);
    }
}
