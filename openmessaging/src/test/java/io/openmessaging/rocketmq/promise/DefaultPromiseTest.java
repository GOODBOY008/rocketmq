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
package io.openmessaging.rocketmq.promise;

import io.openmessaging.Future;
import io.openmessaging.FutureListener;
import io.openmessaging.Promise;
import io.openmessaging.exception.OMSRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;

public class DefaultPromiseTest {
    private Promise<String> promise;

    @BeforeEach
    public void init() {
        promise = new DefaultPromise<>();
    }

    @Test
    public void testIsCancelled() throws Exception {
        Assertions.assertEquals(promise.isCancelled(),false);
    }

    @Test
    public void testIsDone() throws Exception {
        Assertions.assertEquals(promise.isDone(),false);
        promise.set("Done");
        Assertions.assertEquals(promise.isDone(),true);
    }

    @Test
    public void testGet() throws Exception {
        promise.set("Done");
        Assertions.assertEquals(promise.get(),"Done");
    }

    @Test
    public void testGet_WithTimeout() throws Exception {
        try {
            promise.get(100);
            failBecauseExceptionWasNotThrown(OMSRuntimeException.class);
        } catch (OMSRuntimeException e) {
            Assertions.assertEquals(e).hasMessageContaining("Get request result is timeout or interrupted");
        }
    }

    @Test
    public void testAddListener() throws Exception {
        promise.addListener(new FutureListener<String>() {
            @Override
            public void operationComplete(Future<String> future) {
                Assertions.assertEquals(promise.get(),"Done");

            }
        });
        promise.set("Done");
    }

    @Test
    public void testAddListener_ListenerAfterSet() throws Exception {
        promise.set("Done");
        promise.addListener(new FutureListener<String>() {
            @Override
            public void operationComplete(Future<String> future) {
                Assertions.assertEquals(future.get(),"Done");
            }
        });
    }

    @Test
    public void testAddListener_WithException_ListenerAfterSet() throws Exception {
        final Throwable exception = new OMSRuntimeException("-1", "Test Error");
        promise.setFailure(exception);
        promise.addListener(new FutureListener<String>() {
            @Override
            public void operationComplete(Future<String> future) {
                Assertions.assertEquals(promise.getThrowable(),exception);
            }
        });
    }

    @Test
    public void testAddListener_WithException() throws Exception {
        final Throwable exception = new OMSRuntimeException("-1", "Test Error");
        promise.addListener(new FutureListener<String>() {
            @Override
            public void operationComplete(Future<String> future) {
                Assertions.assertEquals(promise.getThrowable(),exception);
            }
        });
        promise.setFailure(exception);
    }

    @Test
    public void getThrowable() throws Exception {
        Assertions.assertNull(promise.getThrowable());
        Throwable exception = new OMSRuntimeException("-1", "Test Error");
        promise.setFailure(exception);
        Assertions.assertEquals(promise.getThrowable(),exception);
    }

}