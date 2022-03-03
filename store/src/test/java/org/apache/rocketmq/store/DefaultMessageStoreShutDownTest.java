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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store;

import java.io.File;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.MockitoJUnitRunner;

import org.junit.jupiter.api.Assertions;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class DefaultMessageStoreShutDownTest {
    private DefaultMessageStore messageStore;

    @BeforeEach
    public void init() throws Exception {
        DefaultMessageStore store = buildMessageStore();
        boolean load = store.load();
        Assertions.assertTrue(load);
        store.start();
        messageStore = spy(store);
        when(messageStore.dispatchBehindBytes()).thenReturn(100L);
    }

    @Test
    public void testDispatchBehindWhenShutdown() {
        messageStore.shutdown();
        Assertions.assertTrue(!messageStore.shutDownNormal);
        File file = new File(StorePathConfigHelper.getAbortFile(messageStore.getMessageStoreConfig().getStorePathRootDir()));
        Assertions.assertTrue(file.exists());
    }

    @AfterEach
    public void destroy() {
        messageStore.destroy();
        File file = new File(messageStore.getMessageStoreConfig().getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    public DefaultMessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMappedFileSizeCommitLog(1024 * 1024 * 10);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024 * 10);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest", true), null, new BrokerConfig());
    }


}
