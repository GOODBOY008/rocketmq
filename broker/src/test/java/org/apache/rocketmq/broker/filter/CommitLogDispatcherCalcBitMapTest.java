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

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.store.DispatchRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;



public class CommitLogDispatcherCalcBitMapTest {

    @Test
    public void testDispatch_filterDataIllegal() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableCalcFilterBitMap(true);

        ConsumerFilterManager filterManager = new ConsumerFilterManager();

        filterManager.register("topic0", "CID_0", "a is not null and a >= 5",
            ExpressionType.SQL92, System.currentTimeMillis());

        filterManager.register("topic0", "CID_1", "a is not null and a >= 15",
            ExpressionType.SQL92, System.currentTimeMillis());

        ConsumerFilterData nullExpression = filterManager.get("topic0", "CID_0");
        nullExpression.setExpression(null);
        nullExpression.setCompiledExpression(null);
        ConsumerFilterData nullBloomData = filterManager.get("topic0", "CID_1");
        nullBloomData.setBloomFilterData(null);

        CommitLogDispatcherCalcBitMap calcBitMap = new CommitLogDispatcherCalcBitMap(brokerConfig,
            filterManager);

        for (int i = 0; i < 1; i++) {
            Map<String, String> properties = new HashMap<String, String>(4);
            properties.put("a", String.valueOf(i * 10 + 5));

            String topic = "topic" + i;

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                0,
                i * 100 + 123,
                100,
                (long) ("tags" + i).hashCode(),
                System.currentTimeMillis(),
                i,
                null,
                UUID.randomUUID().toString(),
                0,
                0,
                properties
            );

            calcBitMap.dispatch(dispatchRequest);

            Assertions.assertNotNull(dispatchRequest.getBitMap());

            BitsArray bitsArray = BitsArray.create(dispatchRequest.getBitMap(),
                filterManager.getBloomFilter().getM());

            for (int j = 0; j < bitsArray.bitLength(); j++) {
                Assertions.assertFalse(bitsArray.getBit(j));
            }
        }
    }

    @Test
    public void testDispatch_blankFilterData() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableCalcFilterBitMap(true);

        ConsumerFilterManager filterManager = new ConsumerFilterManager();

        CommitLogDispatcherCalcBitMap calcBitMap = new CommitLogDispatcherCalcBitMap(brokerConfig,
            filterManager);

        for (int i = 0; i < 10; i++) {
            Map<String, String> properties = new HashMap<String, String>(4);
            properties.put("a", String.valueOf(i * 10 + 5));

            String topic = "topic" + i;

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                0,
                i * 100 + 123,
                100,
                (long) ("tags" + i).hashCode(),
                System.currentTimeMillis(),
                i,
                null,
                UUID.randomUUID().toString(),
                0,
                0,
                properties
            );

            calcBitMap.dispatch(dispatchRequest);

            Assertions.assertNull(dispatchRequest.getBitMap());
        }
    }

    @Test
    public void testDispatch() {
        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setEnableCalcFilterBitMap(true);

        ConsumerFilterManager filterManager = ConsumerFilterManagerTest.gen(10, 10);

        CommitLogDispatcherCalcBitMap calcBitMap = new CommitLogDispatcherCalcBitMap(brokerConfig,
            filterManager);

        for (int i = 0; i < 10; i++) {
            Map<String, String> properties = new HashMap<String, String>(4);
            properties.put("a", String.valueOf(i * 10 + 5));

            String topic = "topic" + i;

            DispatchRequest dispatchRequest = new DispatchRequest(
                topic,
                0,
                i * 100 + 123,
                100,
                (long) ("tags" + i).hashCode(),
                System.currentTimeMillis(),
                i,
                null,
                UUID.randomUUID().toString(),
                0,
                0,
                properties
            );

            calcBitMap.dispatch(dispatchRequest);

            Assertions.assertNotNull(dispatchRequest.getBitMap());

            BitsArray bits = BitsArray.create(dispatchRequest.getBitMap());

            Collection<ConsumerFilterData> filterDatas = filterManager.get(topic);

            for (ConsumerFilterData filterData : filterDatas) {

                if (filterManager.getBloomFilter().isHit(filterData.getBloomFilterData(), bits)) {
                    try {
                        Assertions.assertTrue((Boolean) filterData.getCompiledExpression().evaluate(
                            new MessageEvaluationContext(properties)
                        ));
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assertions.assertFalse(true);
                    }
                } else {
                    try {
                        Assertions.assertFalse((Boolean) filterData.getCompiledExpression().evaluate(
                            new MessageEvaluationContext(properties)
                        ));
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assertions.assertFalse(true);
                    }
                }
            }
        }
    }
}
