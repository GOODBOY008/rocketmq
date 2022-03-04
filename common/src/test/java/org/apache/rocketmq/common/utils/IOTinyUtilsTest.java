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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import static org.junit.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.util.List;

public class IOTinyUtilsTest {

    private String testRootDir = System.getProperty("user.home") + File.separator + "iotinyutilstest";

    @BeforeEach
    public void init() {
        File dir = new File(testRootDir);
        if (dir.exists()) {
            UtilAll.deleteFile(dir);
        }

        dir.mkdirs();
    }

    @AfterEach
    public void destory() {
        File file = new File(testRootDir);
        UtilAll.deleteFile(file);
    }


    @Test
    void testToString() throws Exception {
        byte[] b = "testToString".getBytes(RemotingHelper.DEFAULT_CHARSET);
        InputStream is = new ByteArrayInputStream(b);

        String str = IOTinyUtils.toString(is, null);
        Assertions.assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        str = IOTinyUtils.toString(is, RemotingHelper.DEFAULT_CHARSET);
        Assertions.assertEquals("testToString", str);

        is = new ByteArrayInputStream(b);
        Reader isr = new InputStreamReader(is, RemotingHelper.DEFAULT_CHARSET);
        str = IOTinyUtils.toString(isr);
        Assertions.assertEquals("testToString", str);
    }


    @Test
    void testCopy() throws Exception {
        char[] arr = "testToString".toCharArray();
        Reader reader = new CharArrayReader(arr);
        Writer writer = new CharArrayWriter();

        long count = IOTinyUtils.copy(reader, writer);
        Assertions.assertEquals(arr.length, count);
    }

    @Test
    void testReadLines() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("testReadLines").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        List<String> lines = IOTinyUtils.readLines(reader);

        Assertions.assertEquals(10, lines.size());
    }

    @Test
    void testToBufferedReader() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append("testToBufferedReader").append("\n");
        }

        StringReader reader = new StringReader(sb.toString());
        Method method = IOTinyUtils.class.getDeclaredMethod("toBufferedReader", new Class[]{Reader.class});
        method.setAccessible(true);
        Object bReader = method.invoke(IOTinyUtils.class, reader);

        Assertions.assertTrue(bReader instanceof BufferedReader);
    }

    @Test
    void testWriteStringToFile() throws Exception {
        File file = new File(testRootDir, "testWriteStringToFile");
        Assertions.assertTrue(!file.exists());

        IOTinyUtils.writeStringToFile(file, "testWriteStringToFile", RemotingHelper.DEFAULT_CHARSET);

        Assertions.assertTrue(file.exists());
    }

    @Test
    void testCleanDirectory() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOTinyUtils.writeStringToFile(new File(testRootDir, "testCleanDirectory" + i), "testCleanDirectory", RemotingHelper.DEFAULT_CHARSET);
        }

        File dir = new File(testRootDir);
        Assertions.assertTrue(dir.exists() && dir.isDirectory());
        Assertions.assertTrue(dir.listFiles().length > 0);

        IOTinyUtils.cleanDirectory(new File(testRootDir));

        Assertions.assertTrue(dir.listFiles().length == 0);
    }

    @Test
    void testDelete() throws Exception {
        for (int i = 0; i < 10; i++) {
            IOTinyUtils.writeStringToFile(new File(testRootDir, "testDelete" + i), "testCleanDirectory", RemotingHelper.DEFAULT_CHARSET);
        }

        File dir = new File(testRootDir);
        Assertions.assertTrue(dir.exists() && dir.isDirectory());
        Assertions.assertTrue(dir.listFiles().length > 0);

        IOTinyUtils.delete(new File(testRootDir));

        Assertions.assertTrue(!dir.exists());
    }

    @Test
    void testCopyFile() throws Exception {
        File source = new File(testRootDir, "soruce");
        String target = testRootDir + File.separator + "dest";

        IOTinyUtils.writeStringToFile(source, "testCopyFile", RemotingHelper.DEFAULT_CHARSET);

        IOTinyUtils.copyFile(source.getCanonicalPath(), target);

        File dest = new File(target);
        Assertions.assertTrue(dest.exists());
    }
}
