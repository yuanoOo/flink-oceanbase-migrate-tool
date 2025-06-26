package com.oceanbase.omt.partition;

import java.util.ArrayList;
import java.util.List;

import com.oceanbase.omt.source.clickhouse.ClickHouseType;
import com.oceanbase.omt.utils.DateFormatConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yixing
 */
public class ClickHousePartitionUtils {


    private static final Logger log = LoggerFactory.getLogger(ClickHousePartitionUtils.class);

    public static String buildOBPartitionWithDDL(String ddl, List<PartitionInfo> partitions) {
        if (partitions.isEmpty()) {
            return ddl;
        }
        PartitionInfo partitionInfo = partitions.get(0);
        String partitionKey = partitionInfo.getPartitionKey();
        List<String> partitionKeyType = partitionInfo.getPartitionKeyType();
        String type = partitionKeyType.get(0);
        String partitionKeyExpression = partitionInfo.getPartitionKeyExpression();
        if (ClickHouseType.DateTypes.contains(type)){
            return ddl+handleTimePartition(partitions, partitionKey,type);
        }
        if(partitionKeyExpression.contains("Hash")){
            return ddl+handleHashPartition(partitions, partitionKey);
        }
        return ddl;
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
        StringBuilder listFormat = new StringBuilder();
        for (int i = 0; i < partitions.size(); i++) {
            String partitionName = partitions.get(i).getPartitionName();
            String obPartitionName="p_"+partitionName;
            try {
                String convertPartitionKey = "\""+DateFormatConverter.convertPartitionKey(partitionName,type)+"\"";
                if (i == partitions.size() - 1){
                    listFormat.append(String.format(stepPartition, obPartitionName, convertPartitionKey));
                }else {
                    listFormat.append(String.format(stepPartition, obPartitionName, convertPartitionKey)).append(",");
                }

            }catch (Exception e){
                log.error("convert partition key error", e);
            }
        }
        return String.format(partition,partitionKey, listFormat);

    }

    public static List<String> extract(String input) {
        List<String> result = new ArrayList<>();
        if (input == null || input.isEmpty()) {
            System.out.println("the input is null");
            return result;
        }

        int openParenthesis = input.indexOf('(');
        int closeParenthesis = input.indexOf(')');

        if (openParenthesis == -1 || closeParenthesis == -1 || closeParenthesis < openParenthesis) {
            System.out.println("the input is not valid");
            return result;
        }

        String outside = input.substring(0, openParenthesis);
        String inside = input.substring(openParenthesis + 1, closeParenthesis);
        result.add(outside);
        result.add(inside);
        return result;
    }


}
