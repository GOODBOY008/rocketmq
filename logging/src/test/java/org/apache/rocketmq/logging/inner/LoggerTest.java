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

package org.apache.rocketmq.logging.inner;

import org.apache.rocketmq.logging.BasicLoggerTest;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.junit.jupiter.api.AfterEach;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class LoggerTest extends BasicLoggerTest {


    @BeforeEach
    public void init() {
        InternalLoggerFactory.setCurrentLoggerType(InnerLoggerFactory.LOGGER_INNER);
    }

    @Test
    void testInnerConsoleLogger() throws IOException {
        PrintStream out = System.out;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(byteArrayOutputStream));

        Appender consoleAppender = LoggingBuilder.newAppenderBuilder()
            .withConsoleAppender(LoggingBuilder.SYSTEM_OUT)
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        Logger.getLogger("ConsoleLogger").addAppender(consoleAppender);
        Logger.getLogger("ConsoleLogger").setLevel(Level.INFO);

        InternalLogger consoleLogger1 = InternalLoggerFactory.getLogger("ConsoleLogger");
        consoleLogger1.info("console info Message");
        consoleLogger1.error("console error Message", new RuntimeException());
        consoleLogger1.debug("console debug message");

        consoleLogger1.info("console {} test", "simple");
        consoleLogger1.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", 1, 300);
        consoleLogger1.info("new consumer connected, group: {} {} {} channel: {}", "mygroup", "orderly",
            "broudcast", new RuntimeException("simple object"));

        System.setOut(out);
        consoleAppender.close();

        String result = new String(byteArrayOutputStream.toByteArray());

        System.out.println(result);

        Assertions.assertTrue(result.contains("info"));
        Assertions.assertTrue(result.contains("RuntimeException"));
        Assertions.assertTrue(result.contains("WATERMARK"));
        Assertions.assertTrue(result.contains("consumer"));
        Assertions.assertTrue(result.contains("broudcast"));
        Assertions.assertTrue(result.contains("simple test"));
        Assertions.assertTrue(!result.contains("debug"));
    }

    @Test
    void testInnerFileLogger() throws IOException {
        String file = loggingDir + "/inner.log";

        Logger fileLogger = Logger.getLogger("innerLogger");

        Appender myappender = LoggingBuilder.newAppenderBuilder()
            .withDailyFileRollingAppender(file, "'.'yyyy-MM-dd")
            .withName("innerAppender")
            .withLayout(LoggingBuilder.newLayoutBuilder().withDefaultLayout().build()).build();

        fileLogger.addAppender(myappender);
        fileLogger.setLevel(Level.INFO);

        InternalLogger innerLogger = InternalLoggerFactory.getLogger("innerLogger");

        innerLogger.info("fileLogger info Message");
        innerLogger.error("fileLogger error Message", new RuntimeException());
        innerLogger.debug("fileLogger debug message");

        myappender.close();

        String content = readFile(file);

        System.out.println(content);

        Assertions.assertTrue(content.contains("info"));
        Assertions.assertTrue(content.contains("RuntimeException"));
        Assertions.assertTrue(!content.contains("debug"));
    }

    @AfterEach
    public void close() {
        InternalLoggerFactory.setCurrentLoggerType(null);
    }
}
