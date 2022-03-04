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

package org.apache.rocketmq.test.client.consumer.topic;

import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class MulConsumerMulTopicIT extends BaseConf {
    private RMQNormalProducer producer = null;

    @BeforeEach
    public void setUp() {
        producer = getProducer(nsAddr, null);
    }

    @AfterEach
    public void tearDown() {
        super.shutdown();
    }

    @Test
    void testSynSendMessage() {
        int msgSize = 10;
        String topic1 = initTopic();
        String topic2 = initTopic();
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic1, "*", new RMQNormalListener());
        consumer1.subscribe(topic2, "*");
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic1,
            "*", new RMQNormalListener());
        consumer2.subscribe(topic2, "*");

        producer.send(MQMessageFactory.getMsg(topic1, msgSize));
        producer.send(MQMessageFactory.getMsg(topic2, msgSize));
        Assertions.assertEquals("Not all sent succeeded", msgSize * 2, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        Assertions.assertEquals(recvAll,true);
    }

    @Test
    void testConsumeWithDiffTag() {
        int msgSize = 10;
        String topic1 = initTopic();
        String topic2 = initTopic();
        String tag = "jueyin_tag";
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic1, "*", new RMQNormalListener());
        consumer1.subscribe(topic2, tag);
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic1,
            "*", new RMQNormalListener());
        consumer2.subscribe(topic2, tag);

        producer.send(MQMessageFactory.getMsg(topic1, msgSize));
        producer.send(MQMessageFactory.getMsg(topic2, msgSize, tag));
        Assertions.assertEquals("Not all sent succeeded", msgSize * 2, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        Assertions.assertEquals(recvAll,true);
    }

    @Test
    void testConsumeWithDiffTagAndFilter() {
        int msgSize = 10;
        String topic1 = initTopic();
        String topic2 = initTopic();
        String tag1 = "jueyin_tag_1";
        String tag2 = "jueyin_tag_2";
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic1, "*", new RMQNormalListener());
        consumer1.subscribe(topic2, tag1);
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, topic1, "*", new RMQNormalListener());
        consumer2.subscribe(topic2, tag1);

        producer.send(MQMessageFactory.getMsg(topic2, msgSize, tag2));
        producer.clearMsg();
        producer.send(MQMessageFactory.getMsg(topic1, msgSize));
        producer.send(MQMessageFactory.getMsg(topic2, msgSize, tag1));

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        Assertions.assertEquals(recvAll,true);
    }
}
