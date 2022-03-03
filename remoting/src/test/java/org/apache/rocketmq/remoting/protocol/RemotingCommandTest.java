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
package org.apache.rocketmq.remoting.protocol;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class RemotingCommandTest {
    @Test
    public void testMarkProtocolType_JSONProtocolType() {
        int source = 261;
        SerializeType type = SerializeType.JSON;
        byte[] result = RemotingCommand.markProtocolType(source, type);
        Assertions.assertEquals(result,new byte[] {0, 0, 1, 5});
    }

    @Test
    public void testMarkProtocolType_ROCKETMQProtocolType() {
        int source = 16777215;
        SerializeType type = SerializeType.ROCKETMQ;
        byte[] result = RemotingCommand.markProtocolType(source, type);
        Assertions.assertEquals(result,new byte[] {1, -1, -1, -1});
    }

    @Test
    public void testCreateRequestCommand_RegisterBroker() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        Assertions.assertEquals(cmd.getCode(),code);
        Assertions.assertEquals(cmd.getVersion(),2333);
        Assertions.assertEquals(cmd.getFlag() & 0x01,0); //flag bit 0: 0 presents request
    }

    @Test
    public void testCreateResponseCommand_SuccessWithHeader() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, SampleCommandCustomHeader.class);
        Assertions.assertEquals(cmd.getCode(),code);
        Assertions.assertEquals(cmd.getVersion(),2333);
        Assertions.assertEquals(cmd.getRemark(),remark);
        Assertions.assertEquals(cmd.getFlag() & 0x01,1); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommand_SuccessWithoutHeader() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark);
        Assertions.assertEquals(cmd.getCode(),code);
        Assertions.assertEquals(cmd.getVersion(),2333);
        Assertions.assertEquals(cmd.getRemark(),remark);
        Assertions.assertEquals(cmd.getFlag() & 0x01,1); //flag bit 0: 1 presents response
    }

    @Test
    public void testCreateResponseCommand_FailToCreateCommand() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = RemotingSysResponseCode.SUCCESS;
        String remark = "Sample remark";
        RemotingCommand cmd = RemotingCommand.createResponseCommand(code, remark, CommandCustomHeader.class);
        Assertions.assertNull(cmd);
    }

    @Test
    public void testCreateResponseCommand_SystemError() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        RemotingCommand cmd = RemotingCommand.createResponseCommand(SampleCommandCustomHeader.class);
        Assertions.assertEquals(cmd.getCode(),RemotingSysResponseCode.SYSTEM_ERROR);
        Assertions.assertEquals(cmd.getVersion(),2333);
        Assertions.assertEquals(cmd.getRemark()).contains("not set any response code");
        Assertions.assertEquals(cmd.getFlag() & 0x01,1); //flag bit 0: 1 presents response
    }

    @Test
    public void testEncodeAndDecode_EmptyBody() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            Assertions.assertEquals(decodedCommand.getSerializeTypeCurrentRPC(),SerializeType.JSON);
            Assertions.assertEquals(decodedCommand.getBody());
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assertions.fail("Should not throw IOException");
        }

    }

    @Test
    public void testEncodeAndDecode_FilledBody() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new SampleCommandCustomHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);
        cmd.setBody(new byte[] {0, 1, 2, 3, 4});

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            Assertions.assertEquals(decodedCommand.getSerializeTypeCurrentRPC(),SerializeType.JSON);
            Assertions.assertEquals(decodedCommand.getBody(),new byte[] {0, 1, 2, 3, 4});
        } catch (RemotingCommandException e) {
            e.printStackTrace();
            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    public void testEncodeAndDecode_FilledBodyWithExtFields() throws RemotingCommandException {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        int code = 103; //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        CommandCustomHeader header = new ExtFieldsHeader();
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, header);

        cmd.addExtField("key", "value");

        ByteBuffer buffer = cmd.encode();

        //Simulate buffer being read in NettyDecoder
        buffer.getInt();
        byte[] bytes = new byte[buffer.limit() - 4];
        buffer.get(bytes, 0, buffer.limit() - 4);
        buffer = ByteBuffer.wrap(bytes);

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RemotingCommand.decode(buffer);

            Assertions.assertEquals(decodedCommand.getExtFields().get("stringValue"),"bilibili");
            Assertions.assertEquals(decodedCommand.getExtFields().get("intValue"),"2333");
            Assertions.assertEquals(decodedCommand.getExtFields().get("longValue"),"23333333");
            Assertions.assertEquals(decodedCommand.getExtFields().get("booleanValue"),"true");
            Assertions.assertEquals(decodedCommand.getExtFields().get("doubleValue"),"0.618");

            Assertions.assertEquals(decodedCommand.getExtFields().get("key"),"value");

            CommandCustomHeader decodedHeader = decodedCommand.decodeCommandCustomHeader(ExtFieldsHeader.class);
            Assertions.assertEquals(((ExtFieldsHeader) decodedHeader).getStringValue(),"bilibili");
            Assertions.assertEquals(((ExtFieldsHeader) decodedHeader).getIntValue(),2333);
            Assertions.assertEquals(((ExtFieldsHeader) decodedHeader).getLongValue(),23333333l);
            Assertions.assertEquals(((ExtFieldsHeader) decodedHeader).isBooleanValue(),true);
            Assertions.assertEquals(((ExtFieldsHeader) decodedHeader).getDoubleValue()).isBetween(0.617, 0.619);
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }

    }

    @Test
    public void testNotNullField() throws Exception {
        RemotingCommand remotingCommand = new RemotingCommand();
        Method method = RemotingCommand.class.getDeclaredMethod("isFieldNullable", Field.class);
        method.setAccessible(true);

        Field nullString = FieldTestClass.class.getDeclaredField("nullString");
        Assertions.assertEquals(method.invoke(remotingCommand, nullString),false);

        Field nullableString = FieldTestClass.class.getDeclaredField("nullable");
        Assertions.assertEquals(method.invoke(remotingCommand, nullableString),true);

        Field value = FieldTestClass.class.getDeclaredField("value");
        Assertions.assertEquals(method.invoke(remotingCommand, value),false);
    }
}

class FieldTestClass {
    @CFNotNull
    String nullString = null;

    String nullable = null;

    @CFNotNull
    String value = "NotNull";
}

class SampleCommandCustomHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
    }
}

class ExtFieldsHeader implements CommandCustomHeader {
    private String stringValue = "bilibili";
    private int intValue = 2333;
    private long longValue = 23333333l;
    private boolean booleanValue = true;
    private double doubleValue = 0.618;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getStringValue() {
        return stringValue;
    }

    public int getIntValue() {
        return intValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public boolean isBooleanValue() {
        return booleanValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }
}