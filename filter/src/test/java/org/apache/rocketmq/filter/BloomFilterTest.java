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

package org.apache.rocketmq.filter;

import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

import java.util.Random;



public class BloomFilterTest {

    @Test
    void testEquals() {
        BloomFilter a = BloomFilter.createByFn(10, 20);

        BloomFilter b = BloomFilter.createByFn(10, 20);

        BloomFilter c = BloomFilter.createByFn(12, 20);

        BloomFilter d = BloomFilter.createByFn(10, 30);

        Assertions.assertEquals(a,b);
        Assertions.assertNotEquals(a,c);
        Assertions.assertNotEquals(a,d);
        Assertions.assertNotEquals(d,c);

        Assertions.assertEquals(a.hashCode(),b.hashCode());
        Assertions.assertNotEquals(a.hashCode(),c.hashCode());
        Assertions.assertNotEquals(a.hashCode(),d.hashCode());
        Assertions.assertNotEquals(c.hashCode(),d.hashCode());
    }

    @Test
    void testHashTo() {
        String cid = "CID_abc_efg";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        BitsArray bits = BitsArray.create(bloomFilter.getM());

        int[] bitPos = bloomFilter.calcBitPositions(cid);

        bloomFilter.hashTo(cid, bits);

        for (int bit : bitPos) {
            Assertions.assertTrue(bits.getBit(bit));
        }
    }

    @Test
    void testCalcBitPositions() {
        String cid = "CID_abc_efg";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        int[] bitPos = bloomFilter.calcBitPositions(cid);

        Assertions.assertNotNull(bitPos);
        Assertions.assertEquals(bitPos.length,bloomFilter.getK());

        int[] bitPos2 = bloomFilter.calcBitPositions(cid);

        Assertions.assertNotNull(bitPos2);
        Assertions.assertEquals(bitPos2.length,bloomFilter.getK());

        Assertions.assertEquals(bitPos,bitPos2);
    }

    @Test
    void testIsHit() {
        String cid = "CID_abc_efg";
        String cid2 = "CID_abc_123";

        BloomFilter bloomFilter = BloomFilter.createByFn(10, 20);

        BitsArray bits = BitsArray.create(bloomFilter.getM());

        bloomFilter.hashTo(cid, bits);

        Assertions.assertTrue(bloomFilter.isHit(cid, bits));
        Assertions.assertTrue(!bloomFilter.isHit(cid2, bits));

        bloomFilter.hashTo(cid2, bits);

        Assertions.assertTrue(bloomFilter.isHit(cid, bits));
        Assertions.assertTrue(bloomFilter.isHit(cid2, bits));
    }

    @Test
    void testBloomFilterData() {
        BloomFilterData bloomFilterData = new BloomFilterData(new int[] {1, 2, 3}, 128);
        BloomFilterData bloomFilterData1 = new BloomFilterData(new int[] {1, 2, 3}, 128);
        BloomFilterData bloomFilterData2 = new BloomFilterData(new int[] {1, 2, 3}, 129);

        Assertions.assertEquals(bloomFilterData,bloomFilterData1);
        Assertions.assertNotEquals(bloomFilterData2,bloomFilterData);
        Assertions.assertNotEquals(bloomFilterData2,bloomFilterData1);

        Assertions.assertEquals(bloomFilterData.hashCode(),bloomFilterData1.hashCode());
        Assertions.assertNotEquals(bloomFilterData2.hashCode(),bloomFilterData.hashCode());
        Assertions.assertNotEquals(bloomFilterData2.hashCode(),bloomFilterData1.hashCode());

        Assertions.assertEquals(bloomFilterData.getBitPos(),bloomFilterData2.getBitPos());
        Assertions.assertEquals(bloomFilterData.getBitNum(),bloomFilterData1.getBitNum());
        Assertions.assertNotEquals(bloomFilterData.getBitNum(),bloomFilterData2.getBitNum());

        bloomFilterData2.setBitNum(128);

        Assertions.assertEquals(bloomFilterData,bloomFilterData2);

        bloomFilterData2.setBitPos(new int[] {1, 2, 3, 4});

        Assertions.assertNotEquals(bloomFilterData,bloomFilterData2);

        BloomFilterData nullData = new BloomFilterData();

        Assertions.assertEquals(nullData.getBitNum(),0);
        Assertions.assertNull(nullData.getBitPos());

        BloomFilter bloomFilter = BloomFilter.createByFn(1, 300);

        Assertions.assertNotNull(bloomFilter);
        Assertions.assertFalse(bloomFilter.isValid(bloomFilterData));
    }

    @Test
    void testCheckFalseHit() {
        BloomFilter bloomFilter = BloomFilter.createByFn(1, 300);
        BitsArray bits = BitsArray.create(bloomFilter.getM());
        int falseHit = 0;
        for (int i = 0; i < bloomFilter.getN(); i++) {
            String str = randomString((new Random(System.nanoTime())).nextInt(127) + 10);
            int[] bitPos = bloomFilter.calcBitPositions(str);

            if (bloomFilter.checkFalseHit(bitPos, bits)) {
                falseHit++;
            }

            bloomFilter.hashTo(bitPos, bits);
        }

        Assertions.assertEquals(falseHit).isLessThanOrEqualTo(bloomFilter.getF() * bloomFilter.getN() / 100);
    }

    private String randomString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append((char) ((new Random(System.nanoTime())).nextInt(123 - 97) + 97));
        }

        return stringBuilder.toString();
    }
}
