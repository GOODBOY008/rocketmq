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

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.within;

public class BrokerStatsDataTest {

    @Test
    public void testFromJson() throws Exception {
        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsDay(brokerStatsItem);
        }

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsHour(brokerStatsItem);
        }

        {
            BrokerStatsItem brokerStatsItem = new BrokerStatsItem();
            brokerStatsItem.setAvgpt(10.0);
            brokerStatsItem.setSum(100L);
            brokerStatsItem.setTps(100.0);
            brokerStatsData.setStatsMinute(brokerStatsItem);
        }

        String json = RemotingSerializable.toJson(brokerStatsData, true);
        BrokerStatsData brokerStatsDataResult = RemotingSerializable.fromJson(json, BrokerStatsData.class);

        Assertions.assertEquals(brokerStatsDataResult.getStatsMinute().getAvgpt()).isCloseTo(brokerStatsData.getStatsMinute().getAvgpt(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsMinute().getTps()).isCloseTo(brokerStatsData.getStatsMinute().getTps(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsMinute().getSum(),brokerStatsData.getStatsMinute().getSum());

        Assertions.assertEquals(brokerStatsDataResult.getStatsHour().getAvgpt()).isCloseTo(brokerStatsData.getStatsHour().getAvgpt(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsHour().getTps()).isCloseTo(brokerStatsData.getStatsHour().getTps(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsHour().getSum(),brokerStatsData.getStatsHour().getSum());

        Assertions.assertEquals(brokerStatsDataResult.getStatsDay().getAvgpt()).isCloseTo(brokerStatsData.getStatsDay().getAvgpt(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsDay().getTps()).isCloseTo(brokerStatsData.getStatsDay().getTps(), within(0.0001));
        Assertions.assertEquals(brokerStatsDataResult.getStatsDay().getSum(),brokerStatsData.getStatsDay().getSum());
    }
}
