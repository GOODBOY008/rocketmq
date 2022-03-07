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
package org.apache.rocketmq.client.consumer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.PullAPIWrapper;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoJUnitRunner.class)
public class DefaultMQPullConsumerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    private DefaultMQPullConsumer pullConsumer;
    private String consumerGroup = "FooBarGroup";
    private String topic = "FooBar";
    private String brokerName = "BrokerA";

    @BeforeEach
    public void init() throws Exception {
        pullConsumer = new DefaultMQPullConsumer(consumerGroup);
        pullConsumer.setNamesrvAddr("127.0.0.1:9876");
        pullConsumer.start();
        PullAPIWrapper pullAPIWrapper = pullConsumer.getDefaultMQPullConsumerImpl().getPullAPIWrapper();
        Field field = PullAPIWrapper.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(pullAPIWrapper, mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        when(mQClientFactory.findBrokerAddressInSubscribe(anyString(), anyLong(), anyBoolean())).thenReturn(new FindBrokerResult("127.0.0.1:10911", false));
    }

    @AfterEach
    public void terminate() {
        pullConsumer.shutdown();
    }

    @Test
    void testStart_OffsetShouldNotNUllAfterStart() {
        Assertions.assertNotNull(pullConsumer.getOffsetStore());
    }

    @Test
    void testPullMessage_Success() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                return createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));
            }
        }).when(mQClientAPIImpl).pullMessage(anyString(), any(PullMessageRequestHeader.class), anyLong(), any(CommunicationMode.class), nullable(PullCallback.class));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 1024, 3);
        Assertions.assertNotNull(pullResult);
        Assertions.assertEquals(pullResult.getPullStatus(),PullStatus.FOUND);
        Assertions.assertEquals(pullResult.getNextBeginOffset(),1024 + 1);
        Assertions.assertEquals(pullResult.getMinOffset(),123);
        Assertions.assertEquals(pullResult.getMaxOffset(),2048);
        Assertions.assertEquals(pullResult.getMsgFoundList(),new ArrayList<Object>());
    }

    @Test
    void testPullMessage_NotFound() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                return createPullResult(requestHeader, PullStatus.NO_NEW_MSG, new ArrayList<MessageExt>());
            }
        }).when(mQClientAPIImpl).pullMessage(anyString(), any(PullMessageRequestHeader.class), anyLong(), any(CommunicationMode.class), nullable(PullCallback.class));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 1024, 3);
        Assertions.assertEquals(pullResult.getPullStatus(),PullStatus.NO_NEW_MSG);
    }

    @Test
    void testPullMessageAsync_Success() throws Exception {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock mock) throws Throwable {
                PullMessageRequestHeader requestHeader = mock.getArgument(1);
                PullResult pullResult = createPullResult(requestHeader, PullStatus.FOUND, Collections.singletonList(new MessageExt()));

                PullCallback pullCallback = mock.getArgument(4);
                pullCallback.onSuccess(pullResult);
                return null;
            }
        }).when(mQClientAPIImpl).pullMessage(anyString(), any(PullMessageRequestHeader.class), anyLong(), any(CommunicationMode.class), nullable(PullCallback.class));

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
        pullConsumer.pull(messageQueue, "*", 1024, 3, new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                Assertions.assertNotNull(pullResult);
                Assertions.assertEquals(pullResult.getPullStatus(),PullStatus.FOUND);
                Assertions.assertEquals(pullResult.getNextBeginOffset(),1024 + 1);
                Assertions.assertEquals(pullResult.getMinOffset(),123);
                Assertions.assertEquals(pullResult.getMaxOffset(),2048);
                Assertions.assertEquals(pullResult.getMsgFoundList(),new ArrayList<Object>());
            }

            @Override
            public void onException(Throwable e) {

            }
        });
    }

    private PullResultExt createPullResult(PullMessageRequestHeader requestHeader, PullStatus pullStatus,
        List<MessageExt> messageExtList) throws Exception {
        return new PullResultExt(pullStatus, requestHeader.getQueueOffset() + messageExtList.size(), 123, 2048, messageExtList, 0, new byte[] {});
    }
}