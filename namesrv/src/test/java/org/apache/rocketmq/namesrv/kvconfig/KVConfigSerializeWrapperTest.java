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
package org.apache.rocketmq.namesrv.kvconfig;

import java.util.HashMap;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class KVConfigSerializeWrapperTest {
    private KVConfigSerializeWrapper kvConfigSerializeWrapper;

    @BeforeEach
    public void setup() throws Exception {
        kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
    }

    @Test
    void testEncodeAndDecode() {
        HashMap<String, HashMap<String, String>> result = new HashMap<>();
        HashMap<String, String> kvs = new HashMap<>();
        kvs.put("broker-name", "default-broker");
        kvs.put("topic-name", "default-topic");
        kvs.put("cid", "default-consumer-name");
        result.put(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, kvs);
        kvConfigSerializeWrapper.setConfigTable(result);
        byte[] serializeByte = KVConfigSerializeWrapper.encode(kvConfigSerializeWrapper);
        Assertions.assertNotNull(serializeByte);

        KVConfigSerializeWrapper deserializeObject = KVConfigSerializeWrapper.decode(serializeByte, KVConfigSerializeWrapper.class);
        Assertions.assertEquals(deserializeObject.getConfigTable()).containsKey(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        Assertions.assertEquals(deserializeObject.getConfigTable().get(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG).get("broker-name"),"default-broker");
        Assertions.assertEquals(deserializeObject.getConfigTable().get(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG).get("topic-name"),"default-topic");
        Assertions.assertEquals(deserializeObject.getConfigTable().get(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG).get("cid"),"default-consumer-name");
    }

}