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

package org.apache.rocketmq.common.filter;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;



public class FilterAPITest {
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private String subString = "TAG1 || Tag2 || tag3";

    @Test
    void testBuildSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
                FilterAPI.buildSubscriptionData(topic, subString);
        Assertions.assertEquals(subscriptionData.getTopic(),topic);
        Assertions.assertEquals(subscriptionData.getSubString(),subString);
        String[] tags = subString.split("\\|\\|");
        Set<String> tagSet = new HashSet<String>();
        for (String tag : tags) {
            tagSet.add(tag.trim());
        }
        Assertions.assertEquals(subscriptionData.getTagsSet(),tagSet);
    }

    @Test
    void testBuildTagSome() {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(
                    "TOPIC", "A || B", ExpressionType.TAG
            );

            Assertions.assertNotNull(subscriptionData);
            Assertions.assertEquals(subscriptionData.getTopic(),"TOPIC");
            Assertions.assertEquals(subscriptionData.getSubString(),"A || B");
            Assertions.assertTrue(ExpressionType.isTagType(subscriptionData.getExpressionType()));

            Assertions.assertNotNull(subscriptionData.getTagsSet());
            Assertions.assertEquals(subscriptionData.getTagsSet()).containsExactly("A", "B");
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(Boolean.FALSE);
        }
    }

    @Test
    void testBuildSQL() {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(
                    "TOPIC", "a is not null", ExpressionType.SQL92
            );

            Assertions.assertNotNull(subscriptionData);
            Assertions.assertEquals(subscriptionData.getTopic(),"TOPIC");
            Assertions.assertEquals(subscriptionData.getExpressionType(),ExpressionType.SQL92);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(Boolean.FALSE);
        }
    }

    @Test
    void testBuildSQLWithNullSubString() throws Exception {
        Assertions.assertThrowsExactly(IllegalArgumentException.class,()-> FilterAPI.build("TOPIC", null, ExpressionType.SQL92));
    }
}
