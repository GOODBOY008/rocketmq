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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;



public class QueryConsumeTimeSpanBodyTest {

    @Test
    void testSetGet() throws Exception {
        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> firstQueueTimeSpans = newUniqueConsumeTimeSpanSet();
        List<QueueTimeSpan> secondQueueTimeSpans = newUniqueConsumeTimeSpanSet();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(firstQueueTimeSpans);
        Assertions.assertEquals(queryConsumeTimeSpanBody.getConsumeTimeSpanSet(),firstQueueTimeSpans);
        Assertions.assertNotEquals(queryConsumeTimeSpanBody.getConsumeTimeSpanSet(),secondQueueTimeSpans);
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(secondQueueTimeSpans);
        Assertions.assertEquals(queryConsumeTimeSpanBody.getConsumeTimeSpanSet(),secondQueueTimeSpans);
        Assertions.assertNotEquals(queryConsumeTimeSpanBody.getConsumeTimeSpanSet(),firstQueueTimeSpans);
    }

    @Test
    void testFromJson() throws Exception {
        QueryConsumeTimeSpanBody qctsb = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = new ArrayList<QueueTimeSpan>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(1550825710000l);
        queueTimeSpan.setMaxTimeStamp(1550825790000l);
        queueTimeSpan.setConsumeTimeStamp(1550825760000l);
        queueTimeSpan.setDelayTime(5000l);
        MessageQueue messageQueue = new MessageQueue("topicName", "brokerName", 1);
        queueTimeSpan.setMessageQueue(messageQueue);
        queueTimeSpans.add(queueTimeSpan);
        qctsb.setConsumeTimeSpanSet(queueTimeSpans);
        String json = RemotingSerializable.toJson(qctsb, true);
        QueryConsumeTimeSpanBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeTimeSpanBody.class);
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMaxTimeStamp(),1550825790000l);
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMinTimeStamp(),1550825710000l);
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getDelayTime(),5000l);
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue(),messageQueue);
    }

    @Test
    void testFromJsonRandom() throws Exception {
        QueryConsumeTimeSpanBody origin = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = newUniqueConsumeTimeSpanSet();
        origin.setConsumeTimeSpanSet(queueTimeSpans);
        String json = origin.toJson(true);
        QueryConsumeTimeSpanBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeTimeSpanBody.class);
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMinTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getMinTimeStamp());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMaxTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getMaxTimeStamp());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getDelayTime(),origin.getConsumeTimeSpanSet().get(0).getDelayTime());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic());
        Assertions.assertEquals(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId());
    }

    @Test
    void testEncode() throws Exception {
        QueryConsumeTimeSpanBody origin = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = newUniqueConsumeTimeSpanSet();
        origin.setConsumeTimeSpanSet(queueTimeSpans);
        byte[] data = origin.encode();
        QueryConsumeTimeSpanBody fromData = RemotingSerializable.decode(data, QueryConsumeTimeSpanBody.class);
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getMinTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getMinTimeStamp());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getMaxTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getMaxTimeStamp());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp(),origin.getConsumeTimeSpanSet().get(0).getConsumeTimeStamp());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getDelayTime(),origin.getConsumeTimeSpanSet().get(0).getDelayTime());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getBrokerName());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getTopic());
        Assertions.assertEquals(fromData.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId(),origin.getConsumeTimeSpanSet().get(0).getMessageQueue().getQueueId());
    }

    private List<QueueTimeSpan> newUniqueConsumeTimeSpanSet() {
        List<QueueTimeSpan> queueTimeSpans = new ArrayList<QueueTimeSpan>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(System.currentTimeMillis());
        queueTimeSpan.setMaxTimeStamp(UtilAll.computeNextHourTimeMillis());
        queueTimeSpan.setConsumeTimeStamp(UtilAll.computeNextMinutesTimeMillis());
        queueTimeSpan.setDelayTime(5000l);
        MessageQueue messageQueue = new MessageQueue(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextInt());
        queueTimeSpan.setMessageQueue(messageQueue);
        queueTimeSpans.add(queueTimeSpan);
        return queueTimeSpans;
    }
}
