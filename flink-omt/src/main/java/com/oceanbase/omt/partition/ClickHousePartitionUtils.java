package com.oceanbase.omt.partition;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.oceanbase.omt.source.clickhouse.ClickHouseType;
import com.oceanbase.omt.utils.DateFormatConverter;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yixing
 */
public class ClickHousePartitionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHousePartitionUtils.class);

    private static final Pattern PARTITION_PATTERN = Pattern.compile("\\('([^']*)',(\\d+)\\)");
    private static final String PARTITION_LIST = "PARTITION BY LIST COLUMNS (%s)(%s)";
    private static final String STEP_PARTITION_LIST = "PARTITION %s VALUES IN (%s)";

    private static final String PARTITION_RANGE= "PARTITION BY RANGE COLUMNS (%s)(%s)";
    private static final String STEP_PARTITION_RANGE = "PARTITION %s VALUES LESS THAN (%s)";

    private static final String HASH_PARTITION_FORMAT = "PARTITION BY HASH(%s) PARTITIONS %s";

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
            return appendPartitionDDL(ddl, handleRangePartition(partitions, partitionKey, partitionKeyType));
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
        String listFormat = partitions.stream()
            .map(partition -> String.format(STEP_PARTITION_LIST,
                "p_" + partitions.indexOf(partition),
                "'" + partition.getPartitionName() + "'"))
            .collect(Collectors.joining(","));

        return String.format(PARTITION_LIST, partitionKey, listFormat);
    }

    private static String handleRangePartition(List<PartitionInfo> partitions,String partitionKey,List<String> partitionKeyType){
        StringBuilder listFormat = new StringBuilder();
        List<String> partitionNameList = partitions.stream().map(PartitionInfo::getPartitionName).collect(Collectors.toList());

        List<String> partitionNameListNew = new ArrayList<>();

        List<String> partitionNameListSort = sortPartitionNames(partitionNameList);
        List<String> partitionKeyTypeSort = partitionNameSortType(partitionKeyType);

        List<String> partitionKeyList = Arrays.asList(partitionKey.split(","));
        List<List<String>> lists1 = sortPartitionKeys(partitionKeyType, partitionKeyList);
        List<String> partitionKeyListSort = lists1.get(1);
        String partitionKeyNew = String.join(",", partitionKeyListSort);

        for (int i = 0; i < partitionKeyTypeSort.size(); i++) {
            String s = partitionKeyTypeSort.get(i);
            if (ClickHouseType.DateTypes.contains(s)){
                for (String string : partitionNameListSort) {
                    StringBuilder stringBuilder = new StringBuilder();
                    String substring = string.substring(1, string.length() - 1);
                    String[] split = substring.split(",");
                    List<String> splitList = Arrays.asList(split);
                    List<List<String>> lists = sortPartitionKeys(partitionKeyType, splitList);
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
                listFormat.append(String.format(STEP_PARTITION_RANGE, obPartitionName, partitionName));
            }else {
                listFormat.append(String.format(STEP_PARTITION_RANGE, obPartitionName, partitionName)).append(",");
            }

        }
        return String.format(PARTITION_RANGE,partitionKeyNew, listFormat);

    }


    private static String handleHashPartition(List<PartitionInfo> partitions, String partitionKey) {
        if (partitions == null || partitionKey == null) {
            throw new IllegalArgumentException("partitions and partitionKey cannot be null");
        }
        return String.format(HASH_PARTITION_FORMAT, partitionKey, partitions.size());
    }

    private static String handleTimePartition(List<PartitionInfo> partitions, String partitionColumn,String dateFormatType){
        StringBuilder partitionBuilder = new StringBuilder();
        for (PartitionInfo partitionInfo : partitions) {
            String partitionName = partitionInfo.getPartitionName();
            String obPartitionName = "p_" + partitionName;
            try {
                String convertedKey = "\"" + DateFormatConverter.convertPartitionKey(partitionName, dateFormatType) + "\"";
                if (partitionBuilder.length() > 0) {
                    partitionBuilder.append(",");
                }
                partitionBuilder.append(String.format(STEP_PARTITION_RANGE, obPartitionName, convertedKey));
            } catch (IllegalArgumentException e) {
                LOG.error("Invalid partition key format for partition: {}", partitionName, e);
                throw new IllegalArgumentException("Invalid partition key format: " + partitionName, e);
            } catch (Exception e) {
                LOG.error("Unexpected error while converting partition key for partition: {}", partitionName, e);
                throw new RuntimeException("Failed to convert partition key", e);
            }
        }
        return String.format(PARTITION_RANGE, partitionColumn, partitionBuilder);
    }

    public static List<String> sortPartitionNames(List<String> partitionList){
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

    public static List<List<String>> sortPartitionKeys(List<String> typeList,List<String> partitionNameList){
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
