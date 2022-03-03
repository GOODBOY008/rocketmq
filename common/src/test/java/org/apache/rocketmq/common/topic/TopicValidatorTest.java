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
package org.apache.rocketmq.common.topic;

import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class TopicValidatorTest {

    @Test
    public void testTopicValidator_NotPass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("", response);
        Assertions.assertFalse(res);
        Assertions.assertEquals(response.getCode(),ResponseCode.SYSTEM_ERROR);
        Assertions.assertEquals(response.getRemark()).contains("The specified topic is blank");

        clearResponse(response);
        res = TopicValidator.validateTopic("../TopicTest", response);
        Assertions.assertFalse(res);
        Assertions.assertEquals(response.getCode(),ResponseCode.SYSTEM_ERROR);
        Assertions.assertEquals(response.getRemark()).contains("The specified topic contains illegal characters");

        clearResponse(response);
        res = TopicValidator.validateTopic(generateString(128), response);
        Assertions.assertFalse(res);
        Assertions.assertEquals(response.getCode(),ResponseCode.SYSTEM_ERROR);
        Assertions.assertEquals(response.getRemark()).contains("The specified topic is longer than topic max length.");
    }

    @Test
    public void testTopicValidator_Pass() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        Boolean res = TopicValidator.validateTopic("TestTopic", response);
        Assertions.assertTrue(res);
        Assertions.assertEquals(response.getCode(),-1);
        Assertions.assertEquals(response.getRemark()).isEmpty();
    }

    @Test
    public void testAddSystemTopic() {
        String topic = "SYSTEM_TOPIC_TEST";
        TopicValidator.addSystemTopic(topic);
        Assertions.assertEquals(TopicValidator.getSystemTopicSet()).contains(topic);
    }

    @Test
    public void testIsSystemTopic() {
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic);
            Assertions.assertTrue(res);
        }

        String topic = TopicValidator.SYSTEM_TOPIC_PREFIX + "_test";
        res = TopicValidator.isSystemTopic(topic);
        Assertions.assertTrue(res);

        topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic);
        Assertions.assertFalse(res);
    }

    @Test
    public void testIsSystemTopicWithResponse() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");
        boolean res;
        for (String topic : TopicValidator.getSystemTopicSet()) {
            res = TopicValidator.isSystemTopic(topic, response);
            Assertions.assertTrue(res);
            Assertions.assertEquals(response.getCode(),ResponseCode.SYSTEM_ERROR);
            Assertions.assertEquals(response.getRemark(),"The topic[" + topic + "] is conflict with system topic.");
        }

        String topic = "test_not_system_topic";
        res = TopicValidator.isSystemTopic(topic, response);
        Assertions.assertFalse(res);
    }

    @Test
    public void testIsNotAllowedSendTopic() {
        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic);
            Assertions.assertTrue(res);
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic);
        Assertions.assertFalse(res);
    }

    @Test
    public void testIsNotAllowedSendTopicWithResponse() {
        RemotingCommand response = RemotingCommand.createResponseCommand(-1, "");

        boolean res;
        for (String topic : TopicValidator.getNotAllowedSendTopicSet()) {
            res = TopicValidator.isNotAllowedSendTopic(topic, response);
            Assertions.assertTrue(res);
            Assertions.assertEquals(response.getCode(),ResponseCode.NO_PERMISSION);
            Assertions.assertEquals(response.getRemark(),"Sending message to topic[" + topic + "] is forbidden.");
        }

        String topic = "test_allowed_send_topic";
        res = TopicValidator.isNotAllowedSendTopic(topic, response);
        Assertions.assertFalse(res);
    }

    private static void clearResponse(RemotingCommand response) {
        response.setCode(-1);
        response.setRemark("");
    }

    private static String generateString(int length) {
        StringBuilder stringBuffer = new StringBuilder();
        String tmpStr = "0123456789";
        for (int i = 0; i < length; i++) {
            stringBuffer.append(tmpStr);
        }
        return stringBuffer.toString();
    }
}
