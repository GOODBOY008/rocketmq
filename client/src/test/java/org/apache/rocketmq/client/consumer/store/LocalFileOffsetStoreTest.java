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
package org.apache.rocketmq.client.consumer.store;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class LocalFileOffsetStoreTest {
    @Mock
    private MQClientInstance mQClientFactory;
    private String group = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "DefaultBrokerName";

    @BeforeEach
    public void init() {
        System.setProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("java.io.tmpdir") + File.separator + ".rocketmq_offsets");
        String clientId = new ClientConfig().buildMQClientId() + "#TestNamespace" + System.currentTimeMillis();
        when(mQClientFactory.getClientId()).thenReturn(clientId);
    }

    @Test
    public void testUpdateOffset() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 1);
        offsetStore.updateOffset(messageQueue, 1024, false);

        Assertions.assertEquals(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY),1024);

        offsetStore.updateOffset(messageQueue, 1023, false);
        Assertions.assertEquals(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY),1023);

        offsetStore.updateOffset(messageQueue, 1022, true);
        Assertions.assertEquals(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY),1023);
    }

    @Test
    public void testReadOffset_FromStore() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 2);

        offsetStore.updateOffset(messageQueue, 1024, false);
        Assertions.assertEquals(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE),-1);

        offsetStore.persistAll(new HashSet<MessageQueue>(Collections.singletonList(messageQueue)));
        Assertions.assertEquals(offsetStore.readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE),1024);
    }

    @Test
    public void testCloneOffset() throws Exception {
        OffsetStore offsetStore = new LocalFileOffsetStore(mQClientFactory, group);
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 3);
        offsetStore.updateOffset(messageQueue, 1024, false);
        Map<MessageQueue, Long> cloneOffsetTable = offsetStore.cloneOffsetTable(topic);

        Assertions.assertEquals(cloneOffsetTable.size(),1);
        Assertions.assertEquals(cloneOffsetTable.get(messageQueue),1024);
    }
}