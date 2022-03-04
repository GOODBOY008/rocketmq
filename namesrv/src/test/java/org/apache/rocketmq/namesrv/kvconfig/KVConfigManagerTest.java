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

import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.namesrv.NameServerInstanceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class KVConfigManagerTest extends NameServerInstanceTest {
    private KVConfigManager kvConfigManager;

    @BeforeEach
    public void setup() throws Exception {
        kvConfigManager = new KVConfigManager(nameSrvController);
    }

    @Test
    void testPutKVConfig() {
        kvConfigManager.putKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest", "test");
        byte[] kvConfig = kvConfigManager.getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        Assertions.assertNotNull(kvConfig);
        String value = kvConfigManager.getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest");
        Assertions.assertEquals(value,"test");
    }

    @Test
    void testDeleteKVConfig() {
        kvConfigManager.deleteKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest");
        byte[] kvConfig = kvConfigManager.getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
        Assertions.assertNull(kvConfig);
        Assertions.assertTrue(kvConfig == null);
        String value = kvConfigManager.getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, "UnitTest");
        Assertions.assertNull(value);
    }
}