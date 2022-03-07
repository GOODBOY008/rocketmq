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

package org.apache.rocketmq.test.client.consumer.tag;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.rmq.RMQNormalConsumer;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.test.factory.TagMessage;
import org.apache.rocketmq.test.listener.rmq.concurrent.RMQNormalListener;
import org.apache.rocketmq.test.util.TestUtils;
import org.apache.rocketmq.test.util.VerifyUtils;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class MulTagSubIT extends BaseConf {
    private static Logger logger = Logger.getLogger(TagMessageWith1ConsumerIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @BeforeEach
    public void setUp() {
        topic = initTopic();
        String consumerId = initConsumerGroup();
        logger.info(String.format("use topic: %s; consumerId: %s !", topic, consumerId));
        producer = getProducer(nsAddr, topic);
    }

    @AfterEach
    public void tearDown() {
        super.shutdown();
    }

    @Test
    void testSubTwoTabMessageOnsTag() {
        String tag = "jueyin1";
        String subExpress = String.format("%s||jueyin2", tag);
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());
        producer.send(tag, msgSize);
        Assertions.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());
        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        Assertions.assertEquals(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(producer.getAllMsgBody());
    }

    @Test
    void testSubTwoTabAndMatchOne() {
        String tag1 = "jueyin1";
        String tag2 = "jueyin2";
        String subExpress = String.format("%s||noExistTag", tag2);
        int msgSize = 10;
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        producer.send(tag1, msgSize);
        Assertions.assertEquals("Not all sent succeeded", msgSize, producer.getAllUndupMsgBody().size());
        List<Object> tag2Msgs = MQMessageFactory.getRMQMessage(tag2, topic, msgSize);
        producer.send(tag2Msgs);
        Assertions.assertEquals("Not all sent succeeded", msgSize * 2, producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(MQMessageFactory.getMessageBody(tag2Msgs),
            consumeTime);
        Assertions.assertEquals(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(MQMessageFactory.getMessageBody(tag2Msgs));
    }

    @Test
    void testSubTwoTabAndMatchTwo() {
        String[] tags = {"jueyin1", "jueyin2"};
        String subExpress = String.format("%s||%s", tags[0], tags[1]);
        int msgSize = 10;

        TagMessage tagMessage = new TagMessage(tags, topic, msgSize);
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        producer.send(tagMessage.getMixedTagMessages());
        Assertions.assertEquals("Not all sent succeeded", msgSize * tags.length,
            producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(producer.getAllMsgBody(), consumeTime);

        Assertions.assertEquals(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody()))
            .containsExactlyElementsIn(tagMessage.getAllTagMessageBody());
    }

    @Test
    void testSubThreeTabAndMatchTwo() {
        String[] tags = {"jueyin1", "jueyin2", "jueyin3"};
        String subExpress = String.format("%s||%s", tags[0], tags[1]);
        int msgSize = 10;

        TagMessage tagMessage = new TagMessage(tags, topic, msgSize);
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        producer.send(tagMessage.getMixedTagMessages());
        Assertions.assertEquals("Not all sent succeeded", msgSize * tags.length,
            producer.getAllUndupMsgBody().size());

        consumer.getListener().waitForMessageConsume(
            tagMessage.getMessageBodyByTag(tags[0], tags[1]), consumeTime);

        Assertions.assertEquals(VerifyUtils.getFilterdMessage(producer.getAllMsgBody(),
            consumer.getListener().getAllMsgBody())).containsExactlyElementsIn(
            tagMessage.getMessageBodyByTag(tags[0], tags[1]));
    }

    @Test
    void testNoMatch() {
        String[] tags = {"jueyin1", "jueyin2", "jueyin3"};
        String subExpress = "no_match";
        int msgSize = 10;

        TagMessage tagMessage = new TagMessage(tags, topic, msgSize);
        RMQNormalConsumer consumer = getConsumer(nsAddr, topic, subExpress,
            new RMQNormalListener());

        producer.send(tagMessage.getMixedTagMessages());
        Assertions.assertEquals("Not all sent succeeded", msgSize * tags.length,
            producer.getAllUndupMsgBody().size());

        TestUtils.waitForSeconds(5);

        Assertions.assertEquals(VerifyUtils
            .getFilterdMessage(producer.getAllMsgBody(), consumer.getListener().getAllMsgBody())
            .size(),0);
    }
}
