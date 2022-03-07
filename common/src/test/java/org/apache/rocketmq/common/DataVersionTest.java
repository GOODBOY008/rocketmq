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

package org.apache.rocketmq.common;

import java.util.concurrent.atomic.AtomicLong;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

public class DataVersionTest {

    @Test
    void testEquals() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        Assertions.assertTrue(dataVersion.equals(other));
    }

    @Test
    void testEquals_falseWhenCounterDifferent() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(new AtomicLong(1L));
        other.setTimestamp(dataVersion.getTimestamp());
        Assertions.assertFalse(dataVersion.equals(other));
    }

    @Test
    void testEquals_falseWhenCounterDifferent2() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        Assertions.assertFalse(dataVersion.equals(other));
    }

    @Test
    void testEquals_falseWhenCounterDifferent3() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        Assertions.assertFalse(dataVersion.equals(other));
    }

    @Test
    void testEquals_trueWhenCountersBothNull() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        Assertions.assertTrue(dataVersion.equals(other));
    }
}