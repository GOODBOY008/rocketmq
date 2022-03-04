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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.MockitoJUnitRunner;



@ExtendWith(MockitoJUnitRunner.class)
public class ProcessQueueTest {

    @Test
    void testCachedMessageCount() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        Assertions.assertEquals(pq.getMsgCount().get(),100);

        pq.takeMessages(10);
        pq.commit();

        Assertions.assertEquals(pq.getMsgCount().get(),90);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        Assertions.assertEquals(pq.getMsgCount().get(),89);
    }

    @Test
    void testCachedMessageSize() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        Assertions.assertEquals(pq.getMsgSize().get(),100 * 123);

        pq.takeMessages(10);
        pq.commit();

        Assertions.assertEquals(pq.getMsgSize().get(),90 * 123);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        Assertions.assertEquals(pq.getMsgSize().get(),89 * 123);
    }

    @Test
    void testFillProcessQueueInfo() {
        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(createMessageList(102400));

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        pq.fillProcessQueueInfo(processQueueInfo);

        Assertions.assertEquals(processQueueInfo.getCachedMsgSizeInMiB(),12);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        Assertions.assertEquals(processQueueInfo.getCachedMsgSizeInMiB(),10);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        Assertions.assertEquals(processQueueInfo.getCachedMsgSizeInMiB(),9);

        pq.takeMessages(80000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        Assertions.assertEquals(processQueueInfo.getCachedMsgSizeInMiB(),0);
    }

    private List<MessageExt> createMessageList() {
        return createMessageList(100);
    }

    private List<MessageExt> createMessageList(int count) {
        List<MessageExt> messageExtList = new ArrayList<MessageExt>();
        for (int i = 0; i < count; i++) {
            MessageExt messageExt = new MessageExt();
            messageExt.setQueueOffset(i);
            messageExt.setBody(new byte[123]);
            messageExtList.add(messageExt);
        }
        return messageExtList;
    }
}