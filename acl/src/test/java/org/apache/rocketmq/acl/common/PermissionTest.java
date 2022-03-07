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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

public class PermissionTest {

    @Test
    void fromStringGetPermissionTest() {
        byte perm = Permission.parsePermFromString("PUB");
        Assertions.assertEquals(perm, Permission.PUB);

        perm = Permission.parsePermFromString("SUB");
        Assertions.assertEquals(perm, Permission.SUB);

        perm = Permission.parsePermFromString("PUB|SUB");
        Assertions.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = Permission.parsePermFromString("SUB|PUB");
        Assertions.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = Permission.parsePermFromString("DENY");
        Assertions.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString("1");
        Assertions.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString(null);
        Assertions.assertEquals(perm, Permission.DENY);

    }

    @Test
    void checkPermissionTest() {
        boolean boo = Permission.checkPermission(Permission.DENY, Permission.DENY);
        Assertions.assertFalse(boo);

        boo = Permission.checkPermission(Permission.PUB, Permission.PUB);
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.SUB, Permission.SUB);
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.PUB, (byte) (Permission.PUB|Permission.SUB));
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.SUB, (byte) (Permission.PUB|Permission.SUB));
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, (byte) (Permission.PUB|Permission.SUB));
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.SUB);
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.PUB);
        Assertions.assertTrue(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.ANY);
        Assertions.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.PUB);
        Assertions.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.SUB);
        Assertions.assertFalse(boo);

    }

    @Test
    void setTopicPermTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();

        Permission.parseResourcePerms(plainAccessResource, false, null);
        Assertions.assertNull(resourcePermMap);

        List<String> groups = new ArrayList<>();
        Permission.parseResourcePerms(plainAccessResource, false, groups);
        Assertions.assertNull(resourcePermMap);

        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        Permission.parseResourcePerms(plainAccessResource, false, groups);
        resourcePermMap = plainAccessResource.getResourcePermMap();

        byte perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA"));
        Assertions.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB"));
        Assertions.assertEquals(perm,Permission.PUB|Permission.SUB);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC"));
        Assertions.assertEquals(perm, Permission.PUB);

        List<String> topics = new ArrayList<>();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");

        Permission.parseResourcePerms(plainAccessResource, true, topics);

        perm = resourcePermMap.get("topicA");
        Assertions.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get("topicB");
        Assertions.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = resourcePermMap.get("topicC");
        Assertions.assertEquals(perm, Permission.PUB);

        List<String> erron = new ArrayList<>();
        erron.add("");
        Permission.parseResourcePerms(plainAccessResource, false, erron);
    }

    @Test
    void checkAdminCodeTest() {
        Set<Integer> code = new HashSet<>();
        code.add(17);
        code.add(25);
        code.add(215);
        code.add(200);
        code.add(207);

        for (int i = 0; i < 400; i++) {
            boolean boo = Permission.needAdminPerm(i);
            if (boo) {
                Assertions.assertTrue(code.contains(i));
            }
        }
    }

    @Test
    void AclExceptionTest(){
        AclException aclException = new AclException("CAL_SIGNATURE_FAILED",10015);
        AclException aclExceptionWithMessage = new AclException("CAL_SIGNATURE_FAILED",10015,"CAL_SIGNATURE_FAILED Exception");
        Assertions.assertEquals(aclException.getCode(),10015);
        Assertions.assertEquals(aclExceptionWithMessage.getStatus(),"CAL_SIGNATURE_FAILED");
        aclException.setCode(10016);
        Assertions.assertEquals(aclException.getCode(),10016);
        aclException.setStatus("netaddress examine scope Exception netaddress");
        Assertions.assertEquals(aclException.getStatus(),"netaddress examine scope Exception netaddress");
    }

    @Test
    void checkResourcePermsNormalTest() {
        Permission.checkResourcePerms(null);
        Permission.checkResourcePerms(new ArrayList<>());
        Permission.checkResourcePerms(Arrays.asList("topicA=PUB"));
        Permission.checkResourcePerms(Arrays.asList("topicA=PUB", "topicB=SUB", "topicC=PUB|SUB"));
    }

    @Test
    void checkResourcePermsExceptionTest1() {
        Assertions.assertThrowsExactly(AclException.class,()->Permission.checkResourcePerms(Arrays.asList("topicA")));
    }

    @Test
    void checkResourcePermsExceptionTest2() {
        Assertions.assertThrowsExactly(AclException.class,()->Permission.checkResourcePerms(Arrays.asList("topicA=")));
    }

    @Test
    void checkResourcePermsExceptionTest3() {
        Assertions.assertThrowsExactly(AclException.class,()->Permission.checkResourcePerms(Arrays.asList("topicA=DENY1")));
    }
}
