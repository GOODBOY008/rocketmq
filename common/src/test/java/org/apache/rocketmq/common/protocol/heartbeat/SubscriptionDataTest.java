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

package org.apache.rocketmq.common.protocol.heartbeat;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class SubscriptionDataTest {

    @Test
    public void testConstructor1() {
        SubscriptionData subscriptionData = new SubscriptionData();
        Assertions.assertNull(subscriptionData.getTopic());
        Assertions.assertNull(subscriptionData.getSubString());
        Assertions.assertEquals(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        Assertions.assertEquals(subscriptionData.getExpressionType(),ExpressionType.TAG);
        Assertions.assertNull(subscriptionData.getFilterClassSource());
        Assertions.assertEquals(subscriptionData.getCodeSet()).isEmpty();
        Assertions.assertEquals(subscriptionData.getTagsSet()).isEmpty();
        Assertions.assertFalse(subscriptionData.isClassFilterMode());
    }

    @Test
    public void testConstructor2() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        Assertions.assertEquals(subscriptionData.getTopic(),"TOPICA");
        Assertions.assertEquals(subscriptionData.getSubString(),"*");
        Assertions.assertEquals(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        Assertions.assertEquals(subscriptionData.getExpressionType(),ExpressionType.TAG);
        Assertions.assertNull(subscriptionData.getFilterClassSource());
        Assertions.assertEquals(subscriptionData.getCodeSet()).isEmpty();
        Assertions.assertEquals(subscriptionData.getTagsSet()).isEmpty();
        Assertions.assertFalse(subscriptionData.isClassFilterMode());
    }


    @Test
    public void testHashCodeNotEquals() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        Assertions.assertNotEquals(subscriptionData.hashCode(),System.identityHashCode(subscriptionData));
    }

    @Test
    public void testFromJson() throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setFilterClassSource("TestFilterClassSource");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        String json = RemotingSerializable.toJson(subscriptionData, true);
        SubscriptionData fromJson = RemotingSerializable.fromJson(json, SubscriptionData.class);
        Assertions.assertEquals(subscriptionData,fromJson);
        Assertions.assertEquals(subscriptionData).isEqualByComparingTo(fromJson);
        Assertions.assertEquals(subscriptionData.getFilterClassSource(),"TestFilterClassSource");
        Assertions.assertNull(fromJson.getFilterClassSource());
    }


    @Test
    public void testCompareTo() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        SubscriptionData subscriptionData1 = new SubscriptionData("TOPICBA", "*");
        Assertions.assertEquals(subscriptionData.compareTo(subscriptionData1),"TOPICA@*".compareTo("TOPICB@*"));
    }
}
