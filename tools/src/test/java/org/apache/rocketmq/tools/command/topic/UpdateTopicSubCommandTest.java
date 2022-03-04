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
package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class UpdateTopicSubCommandTest {
    @Test
    void testExecute() {
        UpdateTopicSubCommand cmd = new UpdateTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:10911",
            "-t unit-test",
            "-r 8",
            "-w 8",
            "-p 6",
            "-o false",
            "-u false",
            "-s false"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        Assertions.assertEquals(commandLine.getOptionValue('b').trim(),"127.0.0.1:10911");
        Assertions.assertEquals(commandLine.getOptionValue('r').trim(),"8");
        Assertions.assertEquals(commandLine.getOptionValue('w').trim(),"8");
        Assertions.assertEquals(commandLine.getOptionValue('t').trim(),"unit-test");
        Assertions.assertEquals(commandLine.getOptionValue('p').trim(),"6");
        Assertions.assertEquals(commandLine.getOptionValue('o').trim(),"false");
        Assertions.assertEquals(commandLine.getOptionValue('u').trim(),"false");
        Assertions.assertEquals(commandLine.getOptionValue('s').trim(),"false");
    }
}