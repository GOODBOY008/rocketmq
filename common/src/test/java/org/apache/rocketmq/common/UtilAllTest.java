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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.within;
import org.junit.jupiter.api.Assertions;

public class UtilAllTest {

    @Test
    public void testCurrentStackTrace() {
        String currentStackTrace = UtilAll.currentStackTrace();
        Assertions.assertEquals(currentStackTrace).contains("UtilAll.currentStackTrace");
        Assertions.assertEquals(currentStackTrace).contains("UtilAllTest.testCurrentStackTrace(");
    }

    @Test
    public void testProperties2Object() {
        DemoConfig demoConfig = new DemoConfig();
        Properties properties = new Properties();
        properties.setProperty("demoWidth", "123");
        properties.setProperty("demoLength", "456");
        properties.setProperty("demoOK", "true");
        properties.setProperty("demoName", "TestDemo");
        MixAll.properties2Object(properties, demoConfig);
        Assertions.assertEquals(demoConfig.getDemoLength(),456);
        Assertions.assertEquals(demoConfig.getDemoWidth(),123);
        Assertions.assertTrue(demoConfig.isDemoOK());
        Assertions.assertEquals(demoConfig.getDemoName(),"TestDemo");
    }

    @Test
    public void testProperties2String() {
        DemoConfig demoConfig = new DemoConfig();
        demoConfig.setDemoLength(123);
        demoConfig.setDemoWidth(456);
        demoConfig.setDemoName("TestDemo");
        demoConfig.setDemoOK(true);
        Properties properties = MixAll.object2Properties(demoConfig);
        Assertions.assertEquals(properties.getProperty("demoLength"),"123");
        Assertions.assertEquals(properties.getProperty("demoWidth"),"456");
        Assertions.assertEquals(properties.getProperty("demoOK"),"true");
        Assertions.assertEquals(properties.getProperty("demoName"),"TestDemo");
    }

    @Test
    public void testIsPropertiesEqual() {
        final Properties p1 = new Properties();
        final Properties p2 = new Properties();

        p1.setProperty("a", "1");
        p1.setProperty("b", "2");
        p2.setProperty("a", "1");
        p2.setProperty("b", "2");

        Assertions.assertTrue(MixAll.isPropertiesEqual(p1, p2));
    }

    @Test
    public void testGetPid() {
        Assertions.assertEquals(UtilAll.getPid()).isGreaterThan(0);
    }

    @Test
    public void testGetDiskPartitionSpaceUsedPercent() {
        String tmpDir = System.getProperty("java.io.tmpdir");

        Assertions.assertEquals(UtilAll.getDiskPartitionSpaceUsedPercent(null)).isCloseTo(-1, within(0.000001));
        Assertions.assertEquals(UtilAll.getDiskPartitionSpaceUsedPercent("")).isCloseTo(-1, within(0.000001));
        Assertions.assertEquals(UtilAll.getDiskPartitionSpaceUsedPercent("nonExistingPath")).isCloseTo(-1, within(0.000001));
        Assertions.assertEquals(UtilAll.getDiskPartitionSpaceUsedPercent(tmpDir)).isNotCloseTo(-1, within(0.000001));
    }

    @Test
    public void testIsBlank() {
        Assertions.assertFalse(UtilAll.isBlank("Hello "));
        Assertions.assertFalse(UtilAll.isBlank(" Hello"));
        Assertions.assertFalse(UtilAll.isBlank("He llo"));
        Assertions.assertTrue(UtilAll.isBlank("  "));
        Assertions.assertFalse(UtilAll.isBlank("Hello"));
    }

    @Test
    public void testIPv6Check() throws UnknownHostException {
        InetAddress nonInternal = InetAddress.getByName("2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
        InetAddress internal = InetAddress.getByName("FE80:0000:0000:0000:0000:0000:0000:FFFF");
        Assertions.assertFalse(UtilAll.isInternalV6IP(nonInternal));
        Assertions.assertTrue(UtilAll.isInternalV6IP(internal));
        Assertions.assertEquals(UtilAll.ipToIPv6Str(nonInternal.getAddress()).toUpperCase(),"2408:4004:0180:8100:3FAA:1DDE:2B3F:898A");
    }

    @Test
    public void testJoin() {
        List<String> list = Arrays.asList("groupA=DENY", "groupB=PUB|SUB", "groupC=SUB");
        String comma = ",";
        Assertions.assertEquals("groupA=DENY,groupB=PUB|SUB,groupC=SUB", UtilAll.join(list, comma));
        Assertions.assertEquals(null, UtilAll.join(null, comma));
        Assertions.assertEquals("", UtilAll.join(Collections.emptyList(), comma));
    }

    static class DemoConfig {
        private int demoWidth = 0;
        private int demoLength = 0;
        private boolean demoOK = false;
        private String demoName = "haha";

        int getDemoWidth() {
            return demoWidth;
        }

        public void setDemoWidth(int demoWidth) {
            this.demoWidth = demoWidth;
        }

        public int getDemoLength() {
            return demoLength;
        }

        public void setDemoLength(int demoLength) {
            this.demoLength = demoLength;
        }

        public boolean isDemoOK() {
            return demoOK;
        }

        public void setDemoOK(boolean demoOK) {
            this.demoOK = demoOK;
        }

        public String getDemoName() {
            return demoName;
        }

        public void setDemoName(String demoName) {
            this.demoName = demoName;
        }

        @Override
        public String toString() {
            return "DemoConfig{" +
                "demoWidth=" + demoWidth +
                ", demoLength=" + demoLength +
                ", demoOK=" + demoOK +
                ", demoName='" + demoName + '\'' +
                '}';
        }
    }
}
