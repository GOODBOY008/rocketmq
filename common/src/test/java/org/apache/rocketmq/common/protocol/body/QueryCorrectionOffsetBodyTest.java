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

import java.util.HashMap;
import java.util.Map;



public class QueryCorrectionOffsetBodyTest {

    @Test
    void testFromJson() throws Exception {
        QueryCorrectionOffsetBody qcob = new QueryCorrectionOffsetBody();
        Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
        offsetMap.put(1, 100L);
        offsetMap.put(2, 200L);
        qcob.setCorrectionOffsets(offsetMap);
        String json = RemotingSerializable.toJson(qcob, true);
        QueryCorrectionOffsetBody fromJson = RemotingSerializable.fromJson(json, QueryCorrectionOffsetBody.class);
        Assertions.assertEquals(fromJson.getCorrectionOffsets().get(1),100L);
        Assertions.assertEquals(fromJson.getCorrectionOffsets().get(2),200L);
        Assertions.assertEquals(fromJson.getCorrectionOffsets().size(),2);
    }
}
