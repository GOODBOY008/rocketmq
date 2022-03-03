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

package org.apache.rocketmq.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class MQVersionTest {

    @Test
    public void testGetVersionDesc() throws Exception {
        String desc = "V3_0_0_SNAPSHOT";
        Assertions.assertEquals(MQVersion.getVersionDesc(0),desc);
    }

    @Test
    public void testGetVersionDesc_higherVersion() throws Exception {
        String desc = "HIGHER_VERSION";
        Assertions.assertEquals(MQVersion.getVersionDesc(Integer.MAX_VALUE),desc);
    }

    @Test
    public void testValue2Version() throws Exception {
        Assertions.assertEquals(MQVersion.value2Version(0),MQVersion.Version.V3_0_0_SNAPSHOT);
    }

    @Test
    public void testValue2Version_HigherVersion() throws Exception {
        Assertions.assertEquals(MQVersion.value2Version(Integer.MAX_VALUE),MQVersion.Version.HIGHER_VERSION);
    }
}