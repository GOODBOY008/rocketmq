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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.rocketmq.remoting.protocol.LanguageCode;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class ProducerManagerTest {
    private ProducerManager producerManager;
    private String group = "FooBar";
    private ClientChannelInfo clientInfo;

    @Mock
    private Channel channel;

    @BeforeEach
    public void init() {
        producerManager = new ProducerManager();
        clientInfo = new ClientChannelInfo(channel, "clientId", LanguageCode.JAVA, 0);
    }

    @Test
    void scanNotActiveChannel() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Assertions.assertNotNull((producerManager.getGroupChannelTable().get(group).get(channel)));
        Assertions.assertNotNull(producerManager.findChannel("clientId"));
        Field field = ProducerManager.class.getDeclaredField("CHANNEL_EXPIRED_TIMEOUT");
        field.setAccessible(true);
        long CHANNEL_EXPIRED_TIMEOUT = field.getLong(producerManager);
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis() - CHANNEL_EXPIRED_TIMEOUT - 10);
        when(channel.close()).thenReturn(mock(ChannelFuture.class));
        producerManager.scanNotActiveChannel();
        Assertions.assertNull(producerManager.getGroupChannelTable().get(group).get(channel));
        Assertions.assertNull(producerManager.findChannel("clientId"));
    }

    @Test
    void doChannelCloseEvent() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Assertions.assertNotNull(producerManager.getGroupChannelTable().get(group).get(channel));
        Assertions.assertNotNull(producerManager.findChannel("clientId"));
        producerManager.doChannelCloseEvent("127.0.0.1", channel);
        Assertions.assertNull(producerManager.getGroupChannelTable().get(group).get(channel));
        Assertions.assertNull(producerManager.findChannel("clientId"));
    }

    @Test
    void testRegisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        Channel channel1 = producerManager.findChannel("clientId");
        Assertions.assertNotNull(channelMap);
        Assertions.assertNotNull(channel1);
        Assertions.assertEquals(channelMap.get(channel),clientInfo);
        Assertions.assertEquals(channel1,channel);
    }

    @Test
    void unregisterProducer() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> channelMap = producerManager.getGroupChannelTable().get(group);
        Assertions.assertNotNull(channelMap);
        Assertions.assertEquals(channelMap.get(channel),clientInfo);
        Channel channel1 = producerManager.findChannel("clientId");
        Assertions.assertNotNull(channel1);
        Assertions.assertEquals(channel1,channel);
        producerManager.unregisterProducer(group, clientInfo);
        channelMap = producerManager.getGroupChannelTable().get(group);
        channel1 = producerManager.findChannel("clientId");
        Assertions.assertNull(channelMap);
        Assertions.assertNull(channel1);

    }

    @Test
    void testGetGroupChannelTable() throws Exception {
        producerManager.registerProducer(group, clientInfo);
        Map<Channel, ClientChannelInfo> oldMap = producerManager.getGroupChannelTable().get(group);
        
        producerManager.unregisterProducer(group, clientInfo);
        Assertions.assertEquals(oldMap.size(),0);
    }

    @Test
    void testGetAvailableChannel() {
        producerManager.registerProducer(group, clientInfo);

        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);
        Channel c = producerManager.getAvailableChannel(group);
        Assertions.assertEquals(c).isSameAs(channel);

        when(channel.isWritable()).thenReturn(false);
        c = producerManager.getAvailableChannel(group);
        Assertions.assertEquals(c).isSameAs(channel);

        when(channel.isActive()).thenReturn(false);
        c = producerManager.getAvailableChannel(group);
        Assertions.assertNull(c);
    }

}