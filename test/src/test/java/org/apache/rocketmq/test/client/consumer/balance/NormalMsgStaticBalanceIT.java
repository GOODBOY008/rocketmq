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

package org.apache.rocketmq.test.client.consumer.balance;

import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class NormalMsgStaticBalanceIT extends BaseConf {
    private static Logger logger = Logger.getLogger(NormalMsgStaticBalanceIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @BeforeEach
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s !", topic));
        producer = getProducer(nsAddr, topic);
    }

    @AfterEach
    public void tearDown() {
        super.shutdown();
    }

    @Test
    void testTwoConsumersBalance() {
        int msgSize = 400;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        TestUtils.waitForSeconds(waitTime);

        producer.send(msgSize);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener());
        Assertions.assertEquals(recvAll,true);

        boolean balance = VerifyUtils.verifyBalance(msgSize,
            VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer1.getListener().getAllUndupMsgBody()).size(),
            VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                consumer2.getListener().getAllUndupMsgBody()).size());
        Assertions.assertEquals(balance,true);
    }

    @Test
    void testFourConsumersBalance() {
        int msgSize = 600;
        RMQNormalConsumer consumer1 = getConsumer(nsAddr, topic, "*", new RMQNormalListener());
        RMQNormalConsumer consumer2 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        RMQNormalConsumer consumer3 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        RMQNormalConsumer consumer4 = getConsumer(nsAddr, consumer1.getConsumerGroup(), topic,
            "*", new RMQNormalListener());
        TestUtils.waitForSeconds(waitTime);

        producer.send(msgSize);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        boolean recvAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(),
            consumer1.getListener(), consumer2.getListener(), consumer3.getListener(),
            consumer4.getListener());
        Assertions.assertEquals(recvAll,true);

        boolean balance = VerifyUtils
            .verifyBalance(msgSize,
                VerifyUtils
                    .getFilterdMessage(producer.getAllMsgBody(),
                        consumer1.getListener().getAllUndupMsgBody())
                    .size(),
                VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                    consumer2.getListener().getAllUndupMsgBody()).size(),
                VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                    consumer3.getListener().getAllUndupMsgBody()).size(),
                VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
                    consumer4.getListener().getAllUndupMsgBody()).size());
        Assertions.assertEquals(balance,true);
    }
}
