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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;




public class ConsumeMessageDirectlyResultTest {
    @Test
    void testFromJson() throws Exception {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        boolean defaultAutoCommit = true;
        boolean defaultOrder = false;
        long defaultSpentTimeMills = 1234567L;
        String defaultRemark = "defaultMark";
        CMResult defaultCMResult = CMResult.CR_COMMIT;

        result.setAutoCommit(defaultAutoCommit);
        result.setOrder(defaultOrder);
        result.setRemark(defaultRemark);
        result.setSpentTimeMills(defaultSpentTimeMills);
        result.setConsumeResult(defaultCMResult);

        String json = RemotingSerializable.toJson(result, true);
        ConsumeMessageDirectlyResult fromJson = RemotingSerializable.fromJson(json, ConsumeMessageDirectlyResult.class);
        Assertions.assertNotNull(fromJson);

        Assertions.assertEquals(fromJson.getRemark(),defaultRemark);
        Assertions.assertEquals(fromJson.getSpentTimeMills(),defaultSpentTimeMills);
        Assertions.assertEquals(fromJson.getConsumeResult(),defaultCMResult);
        Assertions.assertEquals(fromJson.isOrder(),defaultOrder);

    }
}
