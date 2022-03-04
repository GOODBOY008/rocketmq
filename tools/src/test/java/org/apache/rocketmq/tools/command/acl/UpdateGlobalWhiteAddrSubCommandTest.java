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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;



public class UpdateGlobalWhiteAddrSubCommandTest {

    @Test
    void testExecute() {
        UpdateGlobalWhiteAddrSubCommand cmd = new UpdateGlobalWhiteAddrSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g 10.10.103.*,192.168.0.*", "-c default-cluster"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        Assertions.assertEquals(commandLine.getOptionValue('g').trim(),"10.10.103.*,192.168.0.*");
        Assertions.assertEquals(commandLine.getOptionValue('c').trim(),"default-cluster");
    }
}
