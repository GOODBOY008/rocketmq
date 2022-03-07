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

package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;



public class QueryConsumeQueueResponseBodyTest {

    @Test
    void test(){
        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();

        SubscriptionData subscriptionData = new SubscriptionData();
        ConsumeQueueData data = new ConsumeQueueData();
        data.setBitMap("defaultBitMap");
        data.setEval(false);
        data.setMsg("this is default msg");
        data.setPhysicOffset(10L);
        data.setPhysicSize(1);
        data.setTagsCode(1L);
        List<ConsumeQueueData> list = new ArrayList<ConsumeQueueData>();
        list.add(data);

        body.setQueueData(list);
        body.setFilterData("default filter data");
        body.setMaxQueueIndex(100L);
        body.setMinQueueIndex(1L);
        body.setSubscriptionData(subscriptionData);

        String json = RemotingSerializable.toJson(body, true);
        QueryConsumeQueueResponseBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeQueueResponseBody.class);
        System.out.println(json);
        //test ConsumeQueue
        ConsumeQueueData jsonData = fromJson.getQueueData().get(0);
        Assertions.assertEquals(jsonData.getMsg(),"this is default msg");
        Assertions.assertEquals(jsonData.getPhysicSize(),1);
        Assertions.assertEquals(jsonData.getBitMap(),"defaultBitMap");
        Assertions.assertEquals(jsonData.getTagsCode(),1L);
        Assertions.assertEquals(jsonData.getPhysicSize(),1);

        //test QueryConsumeQueueResponseBody
        Assertions.assertEquals(fromJson.getFilterData(),"default filter data");
        Assertions.assertEquals(fromJson.getMaxQueueIndex(),100L);
        Assertions.assertEquals(fromJson.getMinQueueIndex(),1L);
        Assertions.assertEquals(fromJson.getSubscriptionData(),subscriptionData);

    }
}
