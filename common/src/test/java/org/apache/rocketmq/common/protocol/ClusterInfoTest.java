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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import static org.junit.Assertions.*;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ClusterInfoTest {

    @Test
    void testFormJson() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertNotNull(json);
        assertNotNull(json.getClusterAddrTable());
        Assertions.assertTrue(json.getClusterAddrTable().containsKey("DEFAULT_CLUSTER"));
        Assertions.assertTrue(json.getClusterAddrTable().get("DEFAULT_CLUSTER").contains("master"));
        assertNotNull(json.getBrokerAddrTable());
        Assertions.assertTrue(json.getBrokerAddrTable().containsKey("master"));
        Assertions.assertEquals(json.getBrokerAddrTable().get("master").getBrokerName(), "master");
        Assertions.assertEquals(json.getBrokerAddrTable().get("master").getCluster(), "DEFAULT_CLUSTER");
        Assertions.assertEquals(json.getBrokerAddrTable().get("master").getBrokerAddrs().get(MixAll.MASTER_ID), MixAll.getLocalhostByNetworkInterface());
    }

    @Test
    void testRetrieveAllClusterNames() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertArrayEquals(new String[]{"DEFAULT_CLUSTER"}, json.retrieveAllClusterNames());
    }


    @Test
    void testRetrieveAllAddrByCluster() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertArrayEquals(new String[]{MixAll.getLocalhostByNetworkInterface()}, json.retrieveAllAddrByCluster("DEFAULT_CLUSTER"));
    }


    private ClusterInfo buildClusterInfo() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<String, Set<String>>();

        //build brokerData
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("master");
        brokerData.setCluster("DEFAULT_CLUSTER");

        //build brokerAddrs
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(MixAll.MASTER_ID, MixAll.getLocalhostByNetworkInterface());

        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put("master", brokerData);

        Set<String> brokerNames = new HashSet<String>();
        brokerNames.add("master");

        clusterAddrTable.put("DEFAULT_CLUSTER", brokerNames);

        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        return clusterInfo;
    }
}