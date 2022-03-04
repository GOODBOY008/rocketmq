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

package org.apache.rocketmq.test.client.producer.querymsg;

import org.apache.log4j.Logger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class QueryMsgByIdExceptionIT extends BaseConf {
    private static Logger logger = Logger.getLogger(QueryMsgByKeyIT.class);
    private static RMQNormalProducer producer = null;
    private static String topic = null;

    @BeforeEach
    public static void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
    }

    @AfterEach
    public static void tearDown() {
        shutdown();
    }

    @Test
    void testQueryMsgByErrorMsgId() {
        producer.clearMsg();
        int msgSize = 20;
        String errorMsgId = "errorMsgId";
        producer.send(msgSize);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        MessageExt queryMsg = null;
        try {
            queryMsg = producer.getProducer().viewMessage(errorMsgId);
        } catch (Exception e) {
        }

        Assertions.assertNull(queryMsg);
    }

    @Test
    void testQueryMsgByNullMsgId() {
        producer.clearMsg();
        int msgSize = 20;
        String errorMsgId = null;
        producer.send(msgSize);
        Assertions.assertEquals("Not all are sent", msgSize, producer.getAllUndupMsgBody().size());

        MessageExt queryMsg = null;
        try {
            queryMsg = producer.getProducer().viewMessage(errorMsgId);
        } catch (Exception e) {
        }

        Assertions.assertNull(queryMsg);
    }
}
