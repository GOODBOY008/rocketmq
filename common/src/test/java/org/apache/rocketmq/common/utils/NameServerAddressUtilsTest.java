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

package org.apache.rocketmq.common.utils;

import org.junit.jupiter.api.Test;



public class NameServerAddressUtilsTest {

    private static String endpoint1 = "http://127.0.0.1:9876";
    private static String endpoint2 = "127.0.0.1:9876";
    private static String endpoint3
        = "http://MQ_INST_123456789_BXXUzaee.xxx:80";
    private static String endpoint4 = "MQ_INST_123456789_BXXUzaee.xxx:80";

    @Test
    void testValidateInstanceEndpoint() {
        Assertions.assertEquals(NameServerAddressUtils.validateInstanceEndpoint(endpoint1),false);
        Assertions.assertEquals(NameServerAddressUtils.validateInstanceEndpoint(endpoint2),false);
        Assertions.assertEquals(NameServerAddressUtils.validateInstanceEndpoint(endpoint3),true);
        Assertions.assertEquals(NameServerAddressUtils.validateInstanceEndpoint(endpoint4),true);
    }

    @Test
    void testParseInstanceIdFromEndpoint() {
        Assertions.assertEquals(NameServerAddressUtils.parseInstanceIdFromEndpoint(endpoint3),
            "MQ_INST_123456789_BXXUzaee");
        Assertions.assertEquals(NameServerAddressUtils.parseInstanceIdFromEndpoint(endpoint4),
            "MQ_INST_123456789_BXXUzaee");
    }

    @Test
    void testGetNameSrvAddrFromNamesrvEndpoint() {
        Assertions.assertEquals(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint1))
            .isEqualTo("127.0.0.1:9876");
        Assertions.assertEquals(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint2))
            .isEqualTo("127.0.0.1:9876");
        Assertions.assertEquals(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint3))
            .isEqualTo("MQ_INST_123456789_BXXUzaee.xxx:80");
        Assertions.assertEquals(NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(endpoint4))
            .isEqualTo("MQ_INST_123456789_BXXUzaee.xxx:80");
    }
}
