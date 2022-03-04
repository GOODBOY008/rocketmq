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

import java.util.HashMap;



public class KVTableTest {

    @Test
    void testFromJson() throws Exception {
        HashMap<String, String> table = new HashMap<String, String>();
        table.put("key1", "value1");
        table.put("key2", "value2");

        KVTable kvTable = new KVTable();
        kvTable.setTable(table);

        String json = RemotingSerializable.toJson(kvTable, true);
        KVTable fromJson = RemotingSerializable.fromJson(json, KVTable.class);

        Assertions.assertNotEquals(fromJson,kvTable);
        Assertions.assertEquals(fromJson.getTable().get("key1"),kvTable.getTable().get("key1"));
        Assertions.assertEquals(fromJson.getTable().get("key2"),kvTable.getTable().get("key2"));
    }

}
