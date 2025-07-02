/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oceanbase.omt.partition;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClickHousePartitionTest {

    @Test
    public void testRangePartitionComplex() {
        String expectedDDL =
                "PARTITION BY RANGE COLUMNS (order_date,region)(PARTITION p_0 VALUES LESS THAN ('2025-07-01','East'),PARTITION p_1 VALUES LESS THAN ('2025-07-01','West'))";
        List<PartitionInfo> partitionInfos = buildRangePartitionComplex();
        String ddl = ClickHousePartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assert.assertEquals(expectedDDL, ddl);
    }

    public List<PartitionInfo> buildRangePartitionComplex() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();
        partitionList.add("String");
        partitionList.add("Date");
        System.out.println(partitionList);

        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1.withPartitionName("('West',202506)");
        partitionInfo1.withPartitionKey("region,order_date");
        partitionInfo1.withPartitionKeyType(partitionList);

        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.withPartitionName("('East',202506)");
        partitionInfo2.withPartitionKey("region,order_date");
        partitionInfo2.withPartitionKeyType(partitionList);

        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        System.out.println(partitionInfo1);
        return partitionInfos;
    }

    @Test
    public void testListSingle() {
        String expectedDDL =
                "PARTITION BY LIST COLUMNS (region)(PARTITION p_0 VALUES IN ('West'),PARTITION p_1 VALUES IN ('East'))";
        List<PartitionInfo> partitionInfos = buildListSinglePartitionInfo();
        String ddl = ClickHousePartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assert.assertEquals(expectedDDL, ddl);
    }

    public List<PartitionInfo> buildListSinglePartitionInfo() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();
        partitionList.add("String");
        System.out.println(partitionList);

        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1.withPartitionName("West");
        partitionInfo1.withPartitionKey("region");
        partitionInfo1.withPartitionKeyType(partitionList);

        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.withPartitionName("East");
        partitionInfo2.withPartitionKey("region");
        partitionInfo2.withPartitionKeyType(partitionList);

        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        System.out.println(partitionInfo1);
        return partitionInfos;
    }

    @Test
    public void testRangePartition() {
        String expectedDDL =
                "PARTITION BY RANGE COLUMNS (order_date)(PARTITION p_202507 VALUES LESS THAN (\"2025-08-01\"),PARTITION p_202506 VALUES LESS THAN (\"2025-07-01\"))";
        List<PartitionInfo> partitionInfos = buildRangePartition();
        String ddl = ClickHousePartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assert.assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testRangeSingle() {
        String expectedDDL =
                "PARTITION BY RANGE COLUMNS (order_date)(PARTITION p_202507 VALUES LESS THAN (\"2025-08-01\"),PARTITION p_202506 VALUES LESS THAN (\"2025-07-01\"))";
        List<PartitionInfo> partitionInfos = buildRangePartition();
        String ddl = ClickHousePartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assert.assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testHash() {
        String expectedDDL = "PARTITION BY HASH(region) PARTITIONS 2";
        List<PartitionInfo> partitionInfos = buildHashPartition();
        String ddl = ClickHousePartitionUtils.buildOBPartitionWithDDL("", partitionInfos);
        Assert.assertEquals(expectedDDL, ddl);
    }

    public List<PartitionInfo> buildRangePartition() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();
        partitionList.add("Date");
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1.withPartitionName("202507");
        partitionInfo1.withPartitionKey("order_date");
        partitionInfo1.withPartitionKeyType(partitionList);
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.withPartitionName("202506");
        partitionInfo2.withPartitionKey("order_date");
        partitionInfo2.withPartitionKeyType(partitionList);

        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    public List<PartitionInfo> buildHashPartition() {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();
        partitionList.add("UInt64");
        PartitionInfo partitionInfo1 = new PartitionInfo();
        partitionInfo1.withPartitionName("0");
        partitionInfo1.withPartitionKey("region");
        partitionInfo1.withPartitionKeyExpression("sipHash64(UserID) % 16");
        partitionInfo1.withPartitionKeyType(partitionList);
        PartitionInfo partitionInfo2 = new PartitionInfo();
        partitionInfo2.withPartitionName("1");
        partitionInfo2.withPartitionKey("region");
        partitionInfo2.withPartitionKeyExpression("sipHash64(UserID) % 16");
        partitionInfo2.withPartitionKeyType(partitionList);

        partitionInfos.add(partitionInfo1);
        partitionInfos.add(partitionInfo2);
        return partitionInfos;
    }

    @Test
    public void partitionSort() {
        String expectedDDL =
                "[('West',202504), ('East',202505), ('South',202505), ('East',202506), ('North',202506), ('South',202506), ('West',202506)]";
        List<String> partitionList = new ArrayList<>();
        partitionList.add("('West',202506)");
        partitionList.add("('East',202505)");
        partitionList.add("('North',202506)");
        partitionList.add("('South',202505)");
        partitionList.add("('West',202504)");
        partitionList.add("('East',202506)");
        partitionList.add("('South',202506)");
        List<String> partitionList1 = ClickHousePartitionUtils.sortPartitionNames(partitionList);
        Assert.assertEquals(expectedDDL, partitionList1.toString());
    }

    @Test
    public void testListSort() {
        List<String> typeList =
                new ArrayList<>(Arrays.asList("String", "Date", "Int32", "DateTime", "Int64"));
        List<String> valueList = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e"));

        List<List<String>> lists = ClickHousePartitionUtils.sortPartitionKeys(typeList, valueList);
        Assert.assertEquals(
                Arrays.asList("Date", "DateTime", "String", "Int32", "Int64"), lists.get(0));
        Assert.assertEquals(Arrays.asList("b", "d", "a", "c", "e"), lists.get(1));
    }
}
