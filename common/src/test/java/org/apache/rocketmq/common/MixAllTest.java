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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;



public class MixAllTest {
    @Test
    void testGetLocalInetAddress() throws Exception {
        List<String> localInetAddress = MixAll.getLocalInetAddress();
        String local = InetAddress.getLocalHost().getHostAddress();
        Assertions.assertEquals(localInetAddress).contains("127.0.0.1");
        Assertions.assertNotNull(local);
    }

    @Test
    void testBrokerVIPChannel() {
        Assertions.assertEquals(MixAll.brokerVIPChannel(true, "127.0.0.1:10911"),"127.0.0.1:10909");
    }

    @Test
    void testCompareAndIncreaseOnly() {
        AtomicLong target = new AtomicLong(5);
        Assertions.assertTrue(MixAll.compareAndIncreaseOnly(target, 6));
        Assertions.assertEquals(target.get(),6);

        Assertions.assertFalse(MixAll.compareAndIncreaseOnly(target, 4));
        Assertions.assertEquals(target.get(),6);
    }

    @Test
    void testFile2String() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForMixAll");
        out.close();
        String string = MixAll.file2String(fileName);
        Assertions.assertEquals(string,"TestForMixAll");
        file.delete();
    }

    @Test
    void testFile2String_WithChinese() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        PrintWriter out = new PrintWriter(fileName);
        out.write("TestForMixAll_中文");
        out.close();
        String string = MixAll.file2String(fileName);
        Assertions.assertEquals(string,"TestForMixAll_中文");
        file.delete();
    }

    @Test
    void testString2File() throws IOException {
        String fileName = System.getProperty("java.io.tmpdir") + File.separator + "MixAllTest" + System.currentTimeMillis();
        MixAll.string2File("MixAll_testString2File", fileName);
        Assertions.assertEquals(MixAll.file2String(fileName),"MixAll_testString2File");
    }

    @Test
    void testGetLocalhostByNetworkInterface() throws Exception {
        Assertions.assertNotNull(MixAll.LOCALHOST);
        Assertions.assertNotNull(MixAll.getLocalhostByNetworkInterface());
    }

    @Test
    void testIsLmq() {
        String testLmq = null;
        Assertions.assertFalse(MixAll.isLmq(testLmq));
        testLmq = "lmq";
        Assertions.assertFalse(MixAll.isLmq(testLmq));
        testLmq = "%LMQ%queue123";
        Assertions.assertTrue(MixAll.isLmq(testLmq));
        testLmq = "%LMQ%GID_TEST";
        Assertions.assertTrue(MixAll.isLmq(testLmq));
    }
}
