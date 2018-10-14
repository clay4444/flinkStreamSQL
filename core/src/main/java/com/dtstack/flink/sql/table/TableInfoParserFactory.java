/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 

package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.enums.ETableType;
import com.dtstack.flink.sql.parser.CreateTableParser;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.StreamSideFactory;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create table statement parsing table structure to obtain specific information
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TableInfoParserFactory {

    private final static String TYPE_KEY = "type";

    //维度表的一个标志，PERIOD FOR SYSTEM_TIME 带有这个的就说明是mysql 维度表
    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    private static Map<String, AbsTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    private static Map<String, AbsTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    private static Map<String, AbsTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    /**
     * 加载插件去解析
     * @param tableType   1 代表 source，2 代表sink
     * @param parserResult   创建 这个表的时候，解析出来的属性
     * @param localPluginRoot  插件地址
     * @return
     * @throws Exception
     */
    public static TableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                               String localPluginRoot) throws Exception {

        // TODO   最顶层的 Table Parser
        AbsTableParser absTableParser = null;

        // with 后面的属性
        Map<String, Object> props = parserResult.getPropMap();

        // 查看和这个源表 / sink表 是什么类型 mysql / kafka
        String type = MathUtil.getString(props.get(TYPE_KEY));

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            //source 源表 处理方式

            // 检查是否是mysql 维度表（ 暂时只支持 mysql 维度表 ）
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            if(!isSideTable){  //不是维度表 ，说明是源表，
                absTableParser = sourceTableInfoMap.get(type);
                if(absTableParser == null){

                    // KafkaSourceParser
                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot);
                    sourceTableInfoMap.put(type, absTableParser);
                }
            }else{  //维度表
                absTableParser = sideTableInfoMap.get(type);
                if(absTableParser == null){
                    // 获取 cache 类型： none，
                    String cacheType = MathUtil.getString(props.get(SideTableInfo.CACHE_KEY));

                    // MysqlSideParser Class
                    absTableParser = StreamSideFactory.getSqlParser(type, localPluginRoot, cacheType);
                    sideTableInfoMap.put(type, absTableParser);
                }
            }

        }else if(tableType == ETableType.SINK.getType()){
            // sink 表 处理方式

            absTableParser = targetTableInfoMap.get(type);
            if(absTableParser == null){
                absTableParser = StreamSinkFactory.getSqlParser(type, localPluginRoot);
                targetTableInfoMap.put(type, absTableParser);
            }
        }

        if(absTableParser == null){
            throw new RuntimeException(String.format("not support %s type of table", type));
        }

        Map<String, Object> prop = Maps.newHashMap();

        //Shield case
        parserResult.getPropMap().forEach((key,val) -> prop.put(key.toLowerCase(), val));

        // 注册成 TableInfo 类 并返回 （FieldsInfo 分割设置类型等 ）
        return absTableParser.getTableInfo(parserResult.getTableName(), parserResult.getFieldsInfoStr(), prop);
    }

    /**
     * judge dim table of PERIOD FOR SYSTEM_TIME
     * 检查当前的源表是否是维度表，判断的标准就是是否含有 PERIOD FOR SYSTEM_TIME 这个标志
     * @param tableField
     * @return
     */
    private static boolean checkIsSideTable(String tableField){
        String[] fieldInfos = tableField.split(",");
        for(String field : fieldInfos){
            Matcher matcher = SIDE_PATTERN.matcher(field.trim());
            if(matcher.find()){
                return true;
            }
        }

        return false;
    }
}
