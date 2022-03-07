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

import java.util.HashMap;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;



public class RocketMQSerializableTest {
    @Test
    void testRocketMQProtocolEncodeAndDecode_WithoutRemarkWithoutExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code, new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        Assertions.assertEquals(result).hasSize(21);
        Assertions.assertEquals(parseToShort(result, 0),(short) code); //code
        Assertions.assertEquals(result[2],LanguageCode.JAVA.getCode()); //language
        Assertions.assertEquals(parseToShort(result, 3),(short) 2333); //version
        Assertions.assertEquals(parseToInt(result, 9),0); //flag
        Assertions.assertEquals(parseToInt(result, 13),0); //empty remark
        Assertions.assertEquals(parseToInt(result, 17),0); //empty extFields

        RemotingCommand decodedCommand = null;
        try {
            decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(result);

            Assertions.assertEquals(decodedCommand.getCode(),code);
            Assertions.assertEquals(decodedCommand.getLanguage(),LanguageCode.JAVA);
            Assertions.assertEquals(decodedCommand.getVersion(),2333);
            Assertions.assertEquals(decodedCommand.getOpaque(),opaque);
            Assertions.assertEquals(decodedCommand.getFlag(),0);
            Assertions.assertNull(decodedCommand.getRemark());
            Assertions.assertNull(decodedCommand.getExtFields());
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void testRocketMQProtocolEncodeAndDecode_WithRemarkWithoutExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code,
            new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);
        cmd.setRemark("Sample Remark");

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        Assertions.assertEquals(result).hasSize(34);
        Assertions.assertEquals(parseToShort(result, 0),(short) code); //code
        Assertions.assertEquals(result[2],LanguageCode.JAVA.getCode()); //language
        Assertions.assertEquals(parseToShort(result, 3),(short) 2333); //version
        Assertions.assertEquals(parseToInt(result, 9),0); //flag
        Assertions.assertEquals(parseToInt(result, 13),13); //remark length

        byte[] remarkArray = new byte[13];
        System.arraycopy(result, 17, remarkArray, 0, 13);
        Assertions.assertEquals(new String(remarkArray),"Sample Remark");

        Assertions.assertEquals(parseToInt(result, 30),0); //empty extFields

        try {
            RemotingCommand decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(result);

            Assertions.assertEquals(decodedCommand.getCode(),code);
            Assertions.assertEquals(decodedCommand.getLanguage(),LanguageCode.JAVA);
            Assertions.assertEquals(decodedCommand.getVersion(),2333);
            Assertions.assertEquals(decodedCommand.getOpaque(),opaque);
            Assertions.assertEquals(decodedCommand.getFlag(),0);
            Assertions.assertEquals(decodedCommand.getRemark()).contains("Sample Remark");
            Assertions.assertNull(decodedCommand.getExtFields());
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void testRocketMQProtocolEncodeAndDecode_WithoutRemarkWithExtFields() {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, "2333");

        //org.apache.rocketmq.common.protocol.RequestCode.REGISTER_BROKER
        int code = 103;
        RemotingCommand cmd = RemotingCommand.createRequestCommand(code,
            new SampleCommandCustomHeader());
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);
        cmd.addExtField("key", "value");

        byte[] result = RocketMQSerializable.rocketMQProtocolEncode(cmd);
        int opaque = cmd.getOpaque();

        Assertions.assertEquals(result).hasSize(35);
        Assertions.assertEquals(parseToShort(result, 0),(short) code); //code
        Assertions.assertEquals(result[2],LanguageCode.JAVA.getCode()); //language
        Assertions.assertEquals(parseToShort(result, 3),(short) 2333); //version
        Assertions.assertEquals(parseToInt(result, 9),0); //flag
        Assertions.assertEquals(parseToInt(result, 13),0); //empty remark
        Assertions.assertEquals(parseToInt(result, 17),14); //extFields length

        byte[] extFieldsArray = new byte[14];
        System.arraycopy(result, 21, extFieldsArray, 0, 14);
        HashMap<String, String> extFields = RocketMQSerializable.mapDeserialize(extFieldsArray);
        Assertions.assertEquals(extFields).contains(new HashMap.SimpleEntry("key", "value"));

        try {
            RemotingCommand decodedCommand = RocketMQSerializable.rocketMQProtocolDecode(result);
            Assertions.assertEquals(decodedCommand.getCode(),code);
            Assertions.assertEquals(decodedCommand.getLanguage(),LanguageCode.JAVA);
            Assertions.assertEquals(decodedCommand.getVersion(),2333);
            Assertions.assertEquals(decodedCommand.getOpaque(),opaque);
            Assertions.assertEquals(decodedCommand.getFlag(),0);
            Assertions.assertNull(decodedCommand.getRemark());
            Assertions.assertEquals(decodedCommand.getExtFields()).contains(new HashMap.SimpleEntry("key", "value"));
        } catch (RemotingCommandException e) {
            e.printStackTrace();

            Assertions.fail("Should not throw IOException");
        }
    }

    @Test
    void testIsBlank_NotBlank() {
        Assertions.assertFalse(RocketMQSerializable.isBlank("bar"));
        Assertions.assertFalse(RocketMQSerializable.isBlank("  A  "));
    }

    @Test
    void testIsBlank_Blank() {
        Assertions.assertTrue(RocketMQSerializable.isBlank(null));
        Assertions.assertTrue(RocketMQSerializable.isBlank(""));
        Assertions.assertTrue(RocketMQSerializable.isBlank("  "));
    }

    private short parseToShort(byte[] array, int index) {
        return (short) (array[index] * 256 + array[++index]);
    }

    private int parseToInt(byte[] array, int index) {
        return array[index] * 16777216 + array[++index] * 65536 + array[++index] * 256
            + array[++index];
    }
}