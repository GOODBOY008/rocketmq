/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class MessageEncodeDecodeTest {

    @Test
    void testEncodeDecodeSingle() throws Exception {
        Message message = new Message("topic", "body".getBytes());
        message.setFlag(12);
        message.putUserProperty("key", "value");
        byte[] bytes = MessageDecoder.encodeMessage(message);
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        Message newMessage = MessageDecoder.decodeMessage(buffer);

        Assertions.assertTrue(message.getFlag() == newMessage.getFlag());
        Assertions.assertTrue(newMessage.getProperty("key").equals(newMessage.getProperty("key")));
        Assertions.assertTrue(Arrays.equals(newMessage.getBody(), message.getBody()));
    }

    @Test
    void testEncodeDecodeList() throws Exception {
        List<Message> messages = new ArrayList<Message>(128);
        for (int i = 0; i < 100; i++) {
            Message message = new Message("topic", ("body" + i).getBytes());
            message.setFlag(i);
            message.putUserProperty("key", "value" + i);
            messages.add(message);
        }
        byte[] bytes = MessageDecoder.encodeMessages(messages);

        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        List<Message> newMsgs = MessageDecoder.decodeMessages(buffer);

        Assertions.assertTrue(newMsgs.size() == messages.size());

        for (int i = 0; i < newMsgs.size(); i++) {
            Message message = messages.get(i);
            Message newMessage = newMsgs.get(i);
            Assertions.assertTrue(message.getFlag() == newMessage.getFlag());
            Assertions.assertTrue(newMessage.getProperty("key").equals(newMessage.getProperty("key")));
            Assertions.assertTrue(Arrays.equals(newMessage.getBody(), message.getBody()));

        }
    }
}
