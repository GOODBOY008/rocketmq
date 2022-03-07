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
package org.apache.rocketmq.acl.common;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.RPCHook;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

public class AclUtilsTest {

    @Test
    void getAddresses() {
        String address = "1.1.1.{1,2,3,4}";
        String[] addressArray = AclUtils.getAddresses(address, "{1,2,3,4}");
        List<String> newAddressList = new ArrayList<>();
        for (String a : addressArray) {
            newAddressList.add(a);
        }

        List<String> addressList = new ArrayList<>();
        addressList.add("1.1.1.1");
        addressList.add("1.1.1.2");
        addressList.add("1.1.1.3");
        addressList.add("1.1.1.4");
        Assertions.assertEquals(newAddressList, addressList);

//        IPv6 test
        String ipv6Address = "1:ac41:9987::bb22:666:{1,2,3,4}";
        String[] ipv6AddressArray = AclUtils.getAddresses(ipv6Address, "{1,2,3,4}");
        List<String> newIPv6AddressList = new ArrayList<>();
        for (String a : ipv6AddressArray) {
            newIPv6AddressList.add(a);
        }

        List<String> ipv6AddressList = new ArrayList<>();
        ipv6AddressList.add("1:ac41:9987::bb22:666:1");
        ipv6AddressList.add("1:ac41:9987::bb22:666:2");
        ipv6AddressList.add("1:ac41:9987::bb22:666:3");
        ipv6AddressList.add("1:ac41:9987::bb22:666:4");
        Assertions.assertEquals(newIPv6AddressList, ipv6AddressList);
    }

    @Test
    void isScopeStringArray() {
        String address = "12";

        for (int i = 0; i < 6; i++) {
            boolean isScope = AclUtils.isScope(address, 4);
            if (i == 3) {
                Assertions.assertTrue(isScope);
            } else {
                Assertions.assertFalse(isScope);
            }
            address = address + ".12";
        }
    }

    @Test
    void isScopeArray() {
        String[] adderss = StringUtils.split("12.12.12.12", ".");
        boolean isScope = AclUtils.isScope(adderss, 4);
        Assertions.assertTrue(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assertions.assertTrue(isScope);

        adderss = StringUtils.split("12.12.1222.1222", ".");
        isScope = AclUtils.isScope(adderss, 4);
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assertions.assertFalse(isScope);

//        IPv6 test
        adderss = StringUtils.split("1050:0000:0000:0000:0005:0600:300c:326b", ":");
        isScope = AclUtils.isIPv6Scope(adderss, 8);
        Assertions.assertTrue(isScope);
        isScope = AclUtils.isIPv6Scope(adderss, 4);
        Assertions.assertTrue(isScope);

        adderss = StringUtils.split("1050:9876:0000:0000:0005:akkg:300c:326b", ":");
        isScope = AclUtils.isIPv6Scope(adderss, 8);
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(adderss, 4);
        Assertions.assertTrue(isScope);

        adderss = StringUtils.split(AclUtils.expandIP("1050::0005:akkg:300c:326b", 8), ":");
        isScope = AclUtils.isIPv6Scope(adderss, 8);
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(adderss, 4);
        Assertions.assertTrue(isScope);

    }

    @Test
    void isScopeStringTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i + "");
            Assertions.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope("-1");
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isScope("256");
        Assertions.assertFalse(isScope);
    }

    @Test
    void isScopeTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i);
            Assertions.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope(-1);
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isScope(256);
        Assertions.assertFalse(isScope);

        //        IPv6 test
        int min = Integer.parseInt("0", 16);
        int max = Integer.parseInt("ffff", 16);
        for (int i = min; i < max + 1; i++) {
            isScope = AclUtils.isIPv6Scope(i);
            Assertions.assertTrue(isScope);
        }
        isScope = AclUtils.isIPv6Scope(-1);
        Assertions.assertFalse(isScope);
        isScope = AclUtils.isIPv6Scope(max + 1);
        Assertions.assertFalse(isScope);

    }

    @Test
    void isAsteriskTest() {
        boolean isAsterisk = AclUtils.isAsterisk("*");
        Assertions.assertTrue(isAsterisk);

        isAsterisk = AclUtils.isAsterisk(",");
        Assertions.assertFalse(isAsterisk);
    }

    @Test
    void isColonTest() {
        boolean isColon = AclUtils.isComma(",");
        Assertions.assertTrue(isColon);

        isColon = AclUtils.isComma("-");
        Assertions.assertFalse(isColon);
    }

    @Test
    void isMinusTest() {
        boolean isMinus = AclUtils.isMinus("-");
        Assertions.assertTrue(isMinus);

        isMinus = AclUtils.isMinus("*");
        Assertions.assertFalse(isMinus);
    }

    @Test
    void v6ipProcessTest() {
        String remoteAddr = "5::7:6:1-200:*";
        String[] strArray = StringUtils.split(remoteAddr, ":");
        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0007:0006");
//        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr, strArray, 3), "0005:0000:0000:0000:0007:0006");

        remoteAddr = "5::7:6:1-200";
        strArray = StringUtils.split(remoteAddr, ":");
        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0000:0007:0006");
//        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr, strArray, 3), "0005:0000:0000:0000:0000:0007:0006");

        remoteAddr = "5::7:6:*";
        strArray = StringUtils.split(remoteAddr, ":");
        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0000:0000:0000:0000:0007:0006");
//        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr, strArray, 3), "0005:0000:0000:0000:0000:0007:0006");

        remoteAddr = "5:7:6:*";
        strArray = StringUtils.split(remoteAddr, ":");
        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr), "0005:0007:0006");
//        Assertions.assertEquals(AclUtils.v6ipProcess(remoteAddr, strArray, 3), "0005:0007:0006");
    }

    @Test
    void expandIPTest() {
        Assertions.assertEquals(AclUtils.expandIP("::", 8), "0000:0000:0000:0000:0000:0000:0000:0000");
        Assertions.assertEquals(AclUtils.expandIP("::1", 8), "0000:0000:0000:0000:0000:0000:0000:0001");
        Assertions.assertEquals(AclUtils.expandIP("3::", 8), "0003:0000:0000:0000:0000:0000:0000:0000");
        Assertions.assertEquals(AclUtils.expandIP("2::2", 8), "0002:0000:0000:0000:0000:0000:0000:0002");
        Assertions.assertEquals(AclUtils.expandIP("4::aac4:92", 8), "0004:0000:0000:0000:0000:0000:AAC4:0092");
        Assertions.assertEquals(AclUtils.expandIP("ab23:56:901a::cc6:765:bb:9011", 8), "AB23:0056:901A:0000:0CC6:0765:00BB:9011");
        Assertions.assertEquals(AclUtils.expandIP("ab23:56:901a:1:cc6:765:bb:9011", 8), "AB23:0056:901A:0001:0CC6:0765:00BB:9011");
        Assertions.assertEquals(AclUtils.expandIP("5::7:6", 6), "0005:0000:0000:0000:0007:0006");
    }

    @SuppressWarnings("unchecked")
    @Test
    void getYamlDataObjectTest() {

        Map<String, Object> map = AclUtils.getYamlDataObject("src/test/resources/conf/plain_acl_correct.yml", Map.class);
        Assertions.assertFalse(map.isEmpty());
    }

    @Test
    void writeDataObject2YamlFileTest() throws IOException {

        String targetFileName = "src/test/resources/conf/plain_write_acl.yml";
        File transport = new File(targetFileName);
        transport.delete();
        transport.createNewFile();

        Map<String, Object> aclYamlMap = new HashMap<String, Object>();

        // For globalWhiteRemoteAddrs element in acl yaml config file
        List<String> globalWhiteRemoteAddrs = new ArrayList<String>();
        globalWhiteRemoteAddrs.add("10.10.103.*");
        globalWhiteRemoteAddrs.add("192.168.0.*");
        aclYamlMap.put("globalWhiteRemoteAddrs", globalWhiteRemoteAddrs);

        // For accounts element in acl yaml config file
        List<Map<String, Object>> accounts = new ArrayList<Map<String, Object>>();
        Map<String, Object> accountsMap = new LinkedHashMap<String, Object>() {
            {
                put("accessKey", "RocketMQ");
                put("secretKey", "12345678");
                put("whiteRemoteAddress", "whiteRemoteAddress");
                put("admin", "true");
            }
        };
        accounts.add(accountsMap);
        aclYamlMap.put("accounts", accounts);
        Assertions.assertTrue(AclUtils.writeDataObject(targetFileName, aclYamlMap));

        transport.delete();
    }

    @Test
    void updateExistedYamlFileTest() throws IOException {

        String targetFileName = "src/test/resources/conf/plain_update_acl.yml";
        File transport = new File(targetFileName);
        transport.delete();
        transport.createNewFile();

        Map<String, Object> aclYamlMap = new HashMap<String, Object>();

        // For globalWhiteRemoteAddrs element in acl yaml config file
        List<String> globalWhiteRemoteAddrs = new ArrayList<String>();
        globalWhiteRemoteAddrs.add("10.10.103.*");
        globalWhiteRemoteAddrs.add("192.168.0.*");
        aclYamlMap.put("globalWhiteRemoteAddrs", globalWhiteRemoteAddrs);

        // Write file to yaml file
        AclUtils.writeDataObject(targetFileName, aclYamlMap);

        Map<String, Object> updatedMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<String> globalWhiteRemoteAddrList = (List<String>) updatedMap.get("globalWhiteRemoteAddrs");
        globalWhiteRemoteAddrList.clear();
        globalWhiteRemoteAddrList.add("192.168.1.2");

        // Update file and flush to yaml file
        AclUtils.writeDataObject(targetFileName, updatedMap);

        Map<String, Object> readableMap = AclUtils.getYamlDataObject(targetFileName, Map.class);
        List<String> updatedGlobalWhiteRemoteAddrs = (List<String>) readableMap.get("globalWhiteRemoteAddrs");
        Assertions.assertEquals("192.168.1.2", updatedGlobalWhiteRemoteAddrs.get(0));

        transport.delete();
    }

    @Test
    void getYamlDataIgnoreFileNotFoundExceptionTest() {

        JSONObject yamlDataObject = AclUtils.getYamlDataObject("plain_acl.yml", JSONObject.class);
        Assertions.assertTrue(yamlDataObject == null);
    }


    @Test
    void getAclRPCHookTest() {

        //RPCHook errorContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_format_error.yml");
        //Assertions.assertNull(errorContRPCHook);

        RPCHook noFileRPCHook = AclUtils.getAclRPCHook("src/test/resources/plain_acl_format_error1.yml");
        Assertions.assertNull(noFileRPCHook);

        //RPCHook emptyContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_null.yml");
        //Assertions.assertNull(emptyContRPCHook);

        RPCHook incompleteContRPCHook = AclUtils.getAclRPCHook("src/test/resources/conf/plain_acl_incomplete.yml");
        Assertions.assertNull(incompleteContRPCHook);
    }

}
