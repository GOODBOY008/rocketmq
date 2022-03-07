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

package org.apache.rocketmq.test.delay;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.test.client.consumer.balance.NormalMsgStaticBalanceIT;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQDelayListner;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NormalMsgDelayIT extends DelayConf {
    private static Logger logger = Logger.getLogger(NormalMsgDelayIT.class);
    protected int msgSize = 100;
    private RMQNormalProducer producer = null;
    private RMQNormalConsumer consumer = null;
    private String topic = null;

    @BeforeEach
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
        consumer = getConsumer(nsAddr, topic, "*", new RMQDelayListner());
    }

    @AfterEach
    public void tearDown() {
        super.shutdown();
    }

    @Test
    void testDelayLevel1() throws Exception {
        Thread.sleep(3000);
        int delayLevel = 1;
        List<Object> delayMsgs = MQMessageFactory.getDelayMsg(topic, delayLevel, msgSize);
        producer.send(delayMsgs);
        Assertions.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);
        Assertions.assertEquals("Not all are consumed", 0, VerifyUtils.verify(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()));
        Assertions.assertEquals("Timer is not correct", true,
            VerifyUtils.verifyDelay(DELAY_LEVEL[delayLevel - 1] * 1000,
                ((RMQDelayListner) consumer.getListener()).getMsgDelayTimes()));
    }

    @Test
    void testDelayLevel2() {
        int delayLevel = 2;
        List<Object> delayMsgs = MQMessageFactory.getDelayMsg(topic, delayLevel, msgSize);
        producer.send(delayMsgs);
        Assertions.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(),
            DELAY_LEVEL[delayLevel - 1] * 1000 * 2);
        Assertions.assertEquals("Not all are consumed", 0, VerifyUtils.verify(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()));
        Assertions.assertEquals("Timer is not correct", true,
            VerifyUtils.verifyDelay(DELAY_LEVEL[delayLevel - 1] * 1000,
                ((RMQDelayListner) consumer.getListener()).getMsgDelayTimes()));
    }

    @Test
    void testDelayLevel3() {
        int delayLevel = 3;
        List<Object> delayMsgs = MQMessageFactory.getDelayMsg(topic, delayLevel, msgSize);
        producer.send(delayMsgs);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(),
            DELAY_LEVEL[delayLevel - 1] * 1000 * 2);
        Assertions.assertEquals("Not all are consumed", 0, VerifyUtils.verify(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()));
        Assertions.assertEquals("Timer is not correct", true,
            VerifyUtils.verifyDelay(DELAY_LEVEL[delayLevel - 1] * 1000,
                ((RMQDelayListner) consumer.getListener()).getMsgDelayTimes()));
    }

    @Test
    void testDelayLevel4() {
        int delayLevel = 4;
        List<Object> delayMsgs = MQMessageFactory.getDelayMsg(topic, delayLevel, msgSize);
        producer.send(delayMsgs);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(),
            DELAY_LEVEL[delayLevel - 1] * 1000 * 2);
        Assertions.assertEquals("Not all are consumed", 0, VerifyUtils.verify(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()));
        Assertions.assertEquals("Timer is not correct", true,
            VerifyUtils.verifyDelay(DELAY_LEVEL[delayLevel - 1] * 1000,
                ((RMQDelayListner) consumer.getListener()).getMsgDelayTimes()));
    }
}
