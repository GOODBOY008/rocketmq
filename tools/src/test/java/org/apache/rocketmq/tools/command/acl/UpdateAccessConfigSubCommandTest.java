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
package org.apache.rocketmq.tools.command.acl;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class UpdateAccessConfigSubCommandTest {

    @Test
    void testExecute() {
        UpdateAccessConfigSubCommand cmd = new UpdateAccessConfigSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:10911",
            "-a RocketMQ",
            "-s 12345678",
            "-w 192.168.0.*",
            "-i DENY",
            "-u SUB",
            "-t topicA=DENY;topicB=PUB|SUB",
            "-g groupA=DENY;groupB=SUB",
            "-m true"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        Assertions.assertEquals(commandLine.getOptionValue('b').trim(),"127.0.0.1:10911");
        Assertions.assertEquals(commandLine.getOptionValue('a').trim(),"RocketMQ");
        Assertions.assertEquals(commandLine.getOptionValue('s').trim(),"12345678");
        Assertions.assertEquals(commandLine.getOptionValue('w').trim(),"192.168.0.*");
        Assertions.assertEquals(commandLine.getOptionValue('i').trim(),"DENY");
        Assertions.assertEquals(commandLine.getOptionValue('u').trim(),"SUB");
        Assertions.assertEquals(commandLine.getOptionValue('t').trim(),"topicA=DENY;topicB=PUB|SUB");
        Assertions.assertEquals(commandLine.getOptionValue('g').trim(),"groupA=DENY;groupB=SUB");
        Assertions.assertEquals(commandLine.getOptionValue('m').trim(),"true");

        PlainAccessConfig accessConfig = new PlainAccessConfig();

        // topicPerms list value
        if (commandLine.hasOption('t')) {
            String[] topicPerms = commandLine.getOptionValue('t').trim().split(";");
            List<String> topicPermList = new ArrayList<String>();
            if (topicPerms != null) {
                for (String topicPerm : topicPerms) {
                    topicPermList.add(topicPerm);
                }
            }
            accessConfig.setTopicPerms(topicPermList);
        }

        // groupPerms list value
        if (commandLine.hasOption('g')) {
            String[] groupPerms = commandLine.getOptionValue('g').trim().split(";");
            List<String> groupPermList = new ArrayList<String>();
            if (groupPerms != null) {
                for (String groupPerm : groupPerms) {
                    groupPermList.add(groupPerm);
                }
            }
            accessConfig.setGroupPerms(groupPermList);
        }

        Assertions.assertTrue(accessConfig.getTopicPerms().contains("topicB=PUB|SUB"));
        Assertions.assertTrue(accessConfig.getGroupPerms().contains("groupB=SUB"));

    }
}
