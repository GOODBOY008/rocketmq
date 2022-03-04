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
package org.apache.rocketmq.common.message;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRACE_SWITCH;
import static org.junit.Assertions.*;

public class MessageTest {
    @Test
    void putUserPropertyWithRuntimeException() throws Exception {
        Assertions.assertThrowsExactly(RuntimeException.class,()->{
            Message m = new Message();

            m.putUserProperty(PROPERTY_TRACE_SWITCH, "");
        });
    }

    @Test
    void putUserNullValuePropertyWithException() throws Exception {
        Assertions.assertThrowsExactly(IllegalArgumentException.class,()->{
            Message m = new Message();

            m.putUserProperty("prop1", null);
        });
    }

    @Test
    void putUserEmptyValuePropertyWithException() throws Exception {
        Assertions.assertThrowsExactly(IllegalArgumentException.class,()->{
            Message m = new Message();

            m.putUserProperty("prop1", "   ");
        });
    }

    @Test
    void putUserNullNamePropertyWithException() throws Exception {
        Assertions.assertThrowsExactly(IllegalArgumentException.class,()->{
            Message m = new Message();

            m.putUserProperty(null, "val1");
        });
    }

    @Test
    void putUserEmptyNamePropertyWithException() throws Exception {
        Assertions.assertThrowsExactly(IllegalArgumentException.class,()->{
            Message m = new Message();

            m.putUserProperty("   ", "val1");
        });
    }

    @Test
    void putUserProperty() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", "val1");
        Assertions.assertEquals("val1", m.getUserProperty("prop1"));
    }
}
