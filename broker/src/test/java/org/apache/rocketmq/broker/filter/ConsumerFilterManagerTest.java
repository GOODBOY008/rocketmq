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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;



public class ConsumerFilterManagerTest {

    public static ConsumerFilterManager gen(int topicCount, int consumerCount) {
        ConsumerFilterManager filterManager = new ConsumerFilterManager();

        for (int i = 0; i < topicCount; i++) {
            String topic = "topic" + i;

            for (int j = 0; j < consumerCount; j++) {

                String consumer = "CID_" + j;

                filterManager.register(topic, consumer, expr(j), ExpressionType.SQL92, System.currentTimeMillis());
            }
        }

        return filterManager;
    }

    public static String expr(int i) {
        return "a is not null and a > " + ((i - 1) * 10) + " and a < " + ((i + 1) * 10);
    }

    @Test
    void testRegister_newExpressionCompileErrorAndRemoveOld() {
        ConsumerFilterManager filterManager = gen(10, 10);

        Assertions.assertNotNull(filterManager.get("topic9", "CID_9"));

        String newExpr = "a between 10,20";

        Assertions.assertFalse(filterManager.register("topic9", "CID_9", newExpr, ExpressionType.SQL92, System.currentTimeMillis() + 1))
            ;
        Assertions.assertNull(filterManager.get("topic9", "CID_9"));

        newExpr = "a between 10 AND 20";

        Assertions.assertTrue(filterManager.register("topic9", "CID_9", newExpr, ExpressionType.SQL92, System.currentTimeMillis() + 1))
            ;

        ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

        Assertions.assertNotNull(filterData);
        Assertions.assertEquals(newExpr,filterData.getExpression());
    }

    @Test
    void testRegister_change() {
        ConsumerFilterManager filterManager = gen(10, 10);

        ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

        System.out.println(filterData.getCompiledExpression());

        String newExpr = "a > 0 and a < 10";

        filterManager.register("topic9", "CID_9", newExpr, ExpressionType.SQL92, System.currentTimeMillis() + 1);

        filterData = filterManager.get("topic9", "CID_9");

        Assertions.assertEquals(newExpr,filterData.getExpression());

        System.out.println(filterData.toString());
    }

    @Test
    void testRegister() {
        ConsumerFilterManager filterManager = gen(10, 10);

        ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

        Assertions.assertNotNull(filterData);
        Assertions.assertFalse(filterData.isDead());

        // new version
        Assertions.assertTrue(filterManager.register(
            "topic9", "CID_9", "a is not null", ExpressionType.SQL92, System.currentTimeMillis() + 1000
        ));

        ConsumerFilterData newFilter = filterManager.get("topic9", "CID_9");

        Assertions.assertNotEquals(newFilter,filterData);

        // same version
        Assertions.assertFalse(filterManager.register(
            "topic9", "CID_9", "a is null", ExpressionType.SQL92, newFilter.getClientVersion()
        ));

        ConsumerFilterData filterData1 = filterManager.get("topic9", "CID_9");

        Assertions.assertEquals(newFilter,filterData1);
    }

    @Test
    void testRegister_reAlive() {
        ConsumerFilterManager filterManager = gen(10, 10);

        ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

        Assertions.assertNotNull(filterData);
        Assertions.assertFalse(filterData.isDead());

        //make dead
        filterManager.unRegister("CID_9");

        //reAlive
        filterManager.register(
            filterData.getTopic(),
            filterData.getConsumerGroup(),
            filterData.getExpression(),
            filterData.getExpressionType(),
            System.currentTimeMillis()
        );

        ConsumerFilterData newFilterData = filterManager.get("topic9", "CID_9");

        Assertions.assertNotNull(newFilterData);
        Assertions.assertFalse(newFilterData.isDead());
    }

    @Test
    void testRegister_bySubscriptionData() {
        ConsumerFilterManager filterManager = new ConsumerFilterManager();
        List<SubscriptionData> subscriptionDatas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            try {
                subscriptionDatas.add(
                    FilterAPI.build(
                        "topic" + i,
                        "a is not null and a > " + i,
                        ExpressionType.SQL92
                    )
                );
            } catch (Exception e) {
                e.printStackTrace();
                Assertions.assertFalse(true);
            }
        }

        filterManager.register("CID_0", subscriptionDatas);

        Collection<ConsumerFilterData> filterDatas = filterManager.getByGroup("CID_0");

        Assertions.assertNotNull(filterDatas);
        Assertions.assertEquals(filterDatas.size(),10);

        Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();

            Assertions.assertNotNull(filterData);
            Assertions.assertTrue(filterManager.getBloomFilter().isValid(filterData.getBloomFilterData()));
        }
    }

    @Test
    void testRegister_tag() {
        ConsumerFilterManager filterManager = new ConsumerFilterManager();

        Assertions.assertFalse(filterManager.register("topic0", "CID_0", "*", null, System.currentTimeMillis()));

        Collection<ConsumerFilterData> filterDatas = filterManager.getByGroup("CID_0");

        Assertions.assertEquals(filterDatas).isNullOrEmpty();
    }

    @Test
    void testUnregister() {
        ConsumerFilterManager filterManager = gen(10, 10);

        ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

        Assertions.assertNotNull(filterData);
        Assertions.assertFalse(filterData.isDead());

        filterManager.unRegister("CID_9");

        Assertions.assertTrue(filterData.isDead());
    }

    @Test
    void testPersist() {
        ConsumerFilterManager filterManager = gen(10, 10);

        try {
            filterManager.persist();

            ConsumerFilterData filterData = filterManager.get("topic9", "CID_9");

            Assertions.assertNotNull(filterData);
            Assertions.assertFalse(filterData.isDead());

            ConsumerFilterManager loadFilter = new ConsumerFilterManager();

            Assertions.assertTrue(loadFilter.load());

            filterData = loadFilter.get("topic9", "CID_9");

            Assertions.assertNotNull(filterData);
            Assertions.assertTrue(filterData.isDead());
            Assertions.assertNotNull(filterData.getCompiledExpression());
        } finally {
            UtilAll.deleteFile(new File("./unit_test"));
        }
    }

    @Test
    void testPersist_clean() {
        ConsumerFilterManager filterManager = gen(10, 10);

        String topic = "topic9";
        for (int i = 0; i < 10; i++) {
            String cid = "CID_" + i;

            ConsumerFilterData filterData = filterManager.get(topic, cid);

            Assertions.assertNotNull(filterData);
            Assertions.assertFalse(filterData.isDead());

            //make dead more than 24h
            filterData.setBornTime(System.currentTimeMillis() - 26 * 60 * 60 * 1000);
            filterData.setDeadTime(System.currentTimeMillis() - 25 * 60 * 60 * 1000);
        }

        try {
            filterManager.persist();

            ConsumerFilterManager loadFilter = new ConsumerFilterManager();

            Assertions.assertTrue(loadFilter.load());

            ConsumerFilterData filterData = loadFilter.get(topic, "CID_9");

            Assertions.assertNull(filterData);

            Collection<ConsumerFilterData> topicData = loadFilter.get(topic);

            Assertions.assertEquals(topicData).isNullOrEmpty();
        } finally {
            UtilAll.deleteFile(new File("./unit_test"));
        }
    }

}
