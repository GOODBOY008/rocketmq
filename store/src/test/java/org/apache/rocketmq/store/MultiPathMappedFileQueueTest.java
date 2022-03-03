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

package org.apache.rocketmq.store;


import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class MultiPathMappedFileQueueTest {

    @Test
    public void testGetLastMappedFile() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
            Assertions.assertNotNull(mappedFile);
            Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
            int idx = i % storePaths.length;
            Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[idx]));
        }
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testLoadReadOnlyMappedFiles() {
        {
            //create old mapped files
            final byte[] fixedMsg = new byte[1024];
            MessageStoreConfig config = new MessageStoreConfig();
            config.setStorePathCommitLog("target/unit_test_store/a/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                    + "target/unit_test_store/b/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                    + "target/unit_test_store/c/");
            MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
            String[] storePaths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            for (int i = 0; i < 1024; i++) {
                MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
                Assertions.assertNotNull(mappedFile);
                Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
                int idx = i % storePaths.length;
                Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[idx]));
            }
            mappedFileQueue.shutdown(1000);
        }

        // test load and readonly
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/b/");
        config.setReadOnlyCommitLogStorePaths("target/unit_test_store/a" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c");
        MultiPathMappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);

        mappedFileQueue.load();

        Assertions.assertEquals(mappedFileQueue.mappedFiles.size(),1024);
        for (int i = 0; i < 1024; i++) {
            Assertions.assertEquals(mappedFileQueue.mappedFiles.get(i).getFile().getName())
                    .isEqualTo(UtilAll.offset2FileName(1024 * i));
        }
        mappedFileQueue.destroy();

    }

    @Test
    public void testUpdatePathsOnline() {
        final byte[] fixedMsg = new byte[1024];

        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, null);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * i);
            Assertions.assertNotNull(mappedFile);
            Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
            int idx = i % storePaths.length;
            Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[idx]));

            if (i == 500) {
                config.setStorePathCommitLog("target/unit_test_store/a/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                        + "target/unit_test_store/b/");
                storePaths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            }
        }
        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }

    @Test
    public void testFullStorePath() {
        final byte[] fixedMsg = new byte[1024];

        Set<String> fullStorePath = new HashSet<>();
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathCommitLog("target/unit_test_store/a/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/b/" + MessageStoreConfig.MULTI_PATH_SPLITTER
                + "target/unit_test_store/c/");
        MappedFileQueue mappedFileQueue = new MultiPathMappedFileQueue(config, 1024, null, () -> fullStorePath);
        String[] storePaths = config.getStorePathCommitLog().trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
        Assertions.assertEquals(storePaths.length,3);

        MappedFile mappedFile = mappedFileQueue.getLastMappedFile(0);
        Assertions.assertNotNull(mappedFile);
        Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
        Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[0]));

        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length);
        Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[1]));
        Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 2);
        Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
        Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[2]));

        fullStorePath.add("target/unit_test_store/b/");
        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 3);
        Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
        Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[2]));

        mappedFile = mappedFileQueue.getLastMappedFile(fixedMsg.length * 4);
        Assertions.assertTrue(mappedFile.appendMessage(fixedMsg));
        Assertions.assertTrue(mappedFile.getFileName().startsWith(storePaths[0]));

        mappedFileQueue.shutdown(1000);
        mappedFileQueue.destroy();
    }
}