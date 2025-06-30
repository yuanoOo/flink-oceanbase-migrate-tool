package com.oceanbase.omt.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.mysql.cj.log.Log;
import com.oceanbase.omt.source.clickhouse.ClickHouseType;
import com.oceanbase.omt.utils.DateFormatConverter;
import lombok.var;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yixing
 */
public class ClickHousePartitionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePartitionUtils.class);

    private static final Pattern PARTITION_PATTERN = Pattern.compile("\\('([^']*)',(\\d+)\\)");
    public static String buildOBPartitionWithDDL(String ddl, List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return ddl;
        }
        PartitionInfo partitionInfo = partitions.get(0);
        String partitionKey = partitionInfo.getPartitionKey();
        List<String> partitionKeyType = partitionInfo.getPartitionKeyType();
        String type = partitionKeyType.get(0);
        String partitionKeyExpression = partitionInfo.getPartitionKeyExpression();

        // Handle multi-column partitioning situation
        if (partitionKeyType.size() > 1) {
            return appendPartitionDDL(ddl, handleListPartition(partitions, partitionKey, partitionKeyType));
        }

        // processing date type partition
        if (ClickHouseType.DateTypes.contains(type)) {
            return appendPartitionDDL(ddl, handleTimePartition(partitions, partitionKey, type));
        }

        // handle hash partitions
        if (StringUtils.isNotBlank(partitionKeyExpression) && partitionKeyExpression.contains("Hash")) {
            return appendPartitionDDL(ddl, handleHashPartition(partitions, partitionKey));
        }
        // Handle single column list partitioning
        return appendPartitionDDL(ddl, handleListPartitionSingle(partitions, partitionKey, partitionKeyType));

    }
    private static String appendPartitionDDL(String ddl, String partitionDDL) {
        return ddl + partitionDDL;
    }
    public static String handleListPartitionSingle(List<PartitionInfo> partitions, String partitionKey, List<String> partitionKeyType) {
        final String partition = "PARTITION BY LIST COLUMNS (%s)(%s)";
        final String stepPartition = "PARTITION %s VALUES IN (%s)";

        StringJoiner listFormat  = new StringJoiner(",");
        for (int i = 0; i < partitions.size(); i++) {
            String partitionName = "'" + partitions.get(i).getPartitionName() + "'";
            String obPartitionName = "p_" + i;
            listFormat.add(String.format(stepPartition, obPartitionName, partitionName));
        }

        return String.format(partition, partitionKey, listFormat);
    }

    private static String handleListPartition(List<PartitionInfo> partitions,String partitionKey,List<String> partitionKeyType){
        String partition = "PARTITION BY RANGE COLUMNS (%s)(%s)";
        String stepPartition = "PARTITION %s VALUES LESS THAN (%s)";
        StringBuilder listFormat = new StringBuilder();
        List<String> partitionNameList = partitions.stream().map(PartitionInfo::getPartitionName).collect(Collectors.toList());
        List<String> partitionNameListNew = new ArrayList<>();
        List<String> partitionNameListSort = sortPartition(partitionNameList);
        List<String> partitionKeyTypeSort = partitionNameSortType(partitionKeyType);
        List<String> partitionKeyList = Arrays.asList(partitionKey.split(","));
        List<List<String>> lists1 = partitionNameSort(partitionKeyType, partitionKeyList);
        List<String> partitionKeyListSort = lists1.get(1);
        String partitionKeyNew = partitionKeyListSort.stream().collect(Collectors.joining(","));

        for (int i = 0; i < partitionKeyTypeSort.size(); i++) {
            String s = partitionKeyTypeSort.get(i);
            if (ClickHouseType.DateTypes.contains(s)){
                for (String string : partitionNameListSort) {
                    StringBuilder stringBuilder = new StringBuilder();
                    String substring = string.substring(1, string.length() - 1);
                    String[] split = substring.split(",");
                    List<String> splitList = Arrays.asList(split);
                    List<List<String>> lists = partitionNameSort(partitionKeyType, splitList);
                    List<String> splitListSort = lists.get(1);
                    String changeStr = splitListSort.get(i);
                    String datStr = DateFormatConverter.convertPartitionKey(changeStr, s);
                    splitListSort.set(i, "'" + datStr + "'");
                    for (int i2 = 0; i2 < splitListSort.size(); i2++) {
                        if (i2 == splitListSort.size() - 1) {
                            stringBuilder.append(splitListSort.get(i2));
                        } else {
                            stringBuilder.append(splitListSort.get(i2)).append(",");
                        }
                    }
                    partitionNameListNew.add(stringBuilder.toString());
                }
            }
        }
        for (int i = 0; i < partitionNameListNew.size(); i++) {
            String partitionName = partitionNameListNew.get(i);
            String obPartitionName = "p_" + i;
            if (i == partitionNameListNew.size() - 1){
                listFormat.append(String.format(stepPartition, obPartitionName, partitionName));
            }else {
                listFormat.append(String.format(stepPartition, obPartitionName, partitionName)).append(",");
            }

        }
        return String.format(partition,partitionKeyNew, listFormat);

    }

    private static String handleHashPartition(List<PartitionInfo> partitions, String partitionKey) {
        String partition = "PARTITION BY HASH(%s)";
        String stepPartition = "PARTITIONS %s";
        int size = partitions.size();
        return String.format(partition, partitionKey)
            + String.format(stepPartition, size);
    }

    private static String handleTimePartition(List<PartitionInfo> partitions, String partitionKey,String type){
        String partition = "PARTITION BY RANGE COLUMNS (%s)(%s)";
        String stepPartition = "PARTITION %s VALUES LESS THAN (%s)";
        StringJoiner partitionJoiner = new StringJoiner(",");
        for (PartitionInfo partitionInfo : partitions) {
            String partitionName = partitionInfo.getPartitionName();
            String obPartitionName = "p_" + partitionName;
            try {
                String convertedKey = "\"" + DateFormatConverter.convertPartitionKey(partitionName, type) + "\"";
                partitionJoiner.add(String.format(stepPartition, obPartitionName, convertedKey));
            } catch (Exception e) {
                LOG.error("Convert partition key error for partition: {}", partitionName, e);
                throw new RuntimeException("Failed to convert partition key", e);
            }
        }
        return String.format(partition,partitionKey, partitionJoiner);
    }

    public static List<String> sortPartition(List<String> partitionList){
        partitionList.sort((s1, s2) -> {
            Matcher m1 = PARTITION_PATTERN.matcher(s1);
            Matcher m2 = PARTITION_PATTERN.matcher(s2);
            if (!m1.find() || !m2.find()) {
                return 0;
            }
            try {
                int num1 = Integer.parseInt(m1.group(2));
                int num2 = Integer.parseInt(m2.group(2));

                // 先比较数字
                int cmp = Integer.compare(num1, num2);
                if (cmp != 0) {
                    return cmp;
                }
                return m1.group(1).compareTo(m2.group(1));
            } catch (NumberFormatException e) {
                return 0;
            }
        });
        return partitionList;
    }

    public static List<List<String>> partitionNameSort(List<String> typeList,List<String> partitionNameList){
        // 使用流式处理将日期类型和非日期类型分开
        Map<Boolean, List<String>> partitionedTypes = IntStream.range(0, typeList.size())
            .boxed()
            .collect(Collectors.partitioningBy(
                i -> ClickHouseType.DateTypes.contains(typeList.get(i)),
                Collectors.mapping(typeList::get, Collectors.toList())
            ));

        Map<Boolean, List<String>> partitionedValues = IntStream.range(0, typeList.size())
            .boxed()
            .collect(Collectors.partitioningBy(
                i -> ClickHouseType.DateTypes.contains(typeList.get(i)),
                Collectors.mapping(partitionNameList::get, Collectors.toList())
            ));

        List<String> sortedTypeList = new ArrayList<>();
        sortedTypeList.addAll(partitionedTypes.get(true));
        sortedTypeList.addAll(partitionedTypes.get(false));

        List<String> sortedValueList = new ArrayList<>();
        sortedValueList.addAll(partitionedValues.get(true));
        sortedValueList.addAll(partitionedValues.get(false));

        return Arrays.asList(sortedTypeList, sortedValueList);
    }

    public static List<String> partitionNameSortType(List<String> typeList){
        // 使用流式处理将日期类型和非日期类型分开
        Map<Boolean, List<String>> partitionedTypes = IntStream.range(0, typeList.size())
            .boxed()
            .collect(Collectors.partitioningBy(
                i -> ClickHouseType.DateTypes.contains(typeList.get(i)),
                Collectors.mapping(typeList::get, Collectors.toList())
            ));

        List<String> sortedTypeList = new ArrayList<>();
        sortedTypeList.addAll(partitionedTypes.get(true));
        sortedTypeList.addAll(partitionedTypes.get(false));
        return sortedTypeList;
    }



}
