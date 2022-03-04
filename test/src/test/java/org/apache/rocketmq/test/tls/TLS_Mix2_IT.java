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

package org.apache.rocketmq.test.tls;

import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.MQWait;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TLS_Mix2_IT extends BaseConf {

    private RMQNormalProducer producer;
    private RMQNormalConsumer consumer;

    private String topic;

    @BeforeEach
    public void setUp() {
        topic = initTopic();
        // send message via TLS
        producer = getProducer(nsAddr, topic, true);

        // Receive message without TLS.
        consumer = getConsumer(nsAddr, topic, "*", new RMQNormalListener(), false);
    }

    @AfterEach
    public void tearDown() {
        shutdown();
    }

    @Test
    void testSendAndReceiveMessageOverTLS() {
        int numberOfMessagesToSend = 16;
        producer.send(numberOfMessagesToSend);

        boolean consumedAll = MQWait.waitConsumeAll(consumeTime, producer.getAllMsgBody(), consumer.getListener());
        Assertions.assertTrue(consumedAll);
    }

}
