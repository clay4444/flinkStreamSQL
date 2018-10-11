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

 

package com.dtstack.flink.sql.parser;

import com.dtstack.flink.sql.enums.ETableType;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.table.TableInfoParserFactory;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

import java.util.List;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SqlParser {

    private static final char SQL_DELIMITER = ';';

    private static String LOCAL_SQL_PLUGIN_ROOT;   //本地plugin 打包插件  本地启动时，设置的

    // 构建所有的解析器集合：创建自定义函数的parser，创建表的parser，插入数据的parser
    private static List<IParser> sqlParserList = Lists.newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance());

    public static void setLocalSqlPluginRoot(String localSqlPluginRoot){
        LOCAL_SQL_PLUGIN_ROOT = localSqlPluginRoot;
    }

    /**
     * flink 支持的sql 语法
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.dtstack.com;
     * insert into tb1 select * from tb2;
     * @param sql
     */
    public static SqlTree parseSql(String sql) throws Exception {

        if(StringUtils.isBlank(sql)){  // sql 非空
            throw new RuntimeException("sql is not null");
        }

        if(LOCAL_SQL_PLUGIN_ROOT == null){  // 插件打包的目录
            throw new RuntimeException("need to set local sql plugin root");
        }

        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replace("\t", " ").trim();  // 格式化

        //  根据  ; 分割 sql  获取sql 集合
        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);

        //构建sql 语法树
        SqlTree sqlTree = new SqlTree();

        for(String childSql : sqlArr){
            if(Strings.isNullOrEmpty(childSql)){
                continue;
            }
            boolean result = false;
            //验证应该使用哪种parser 解析该sql
            for(IParser sqlParser : sqlParserList){
                if(!sqlParser.verify(childSql)){
                    continue;  // 直接跳过
                }

                sqlParser.parseSql(childSql, sqlTree);
                result = true;
            }

            if(!result){  // result 为false，说明 没有对应的解析器，直接抛异常
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }

        //解析exec-sql
        if(sqlTree.getExecSqlList().size() == 0){
            throw new RuntimeException("sql no executable statement");
        }

        for(InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()){
            List<String> sourceTableList = result.getSourceTableList();
            List<String> targetTableList = result.getTargetTableList();

            for(String tableName : sourceTableList){
                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                if(createTableResult == null){
                    throw new RuntimeException("can't find table " + tableName);
                }

                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SOURCE.getType(),
                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                sqlTree.addTableInfo(tableName, tableInfo);
            }

            for(String tableName : targetTableList){
                CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                if(createTableResult == null){
                    throw new RuntimeException("can't find table " + tableName);
                }

                TableInfo tableInfo = TableInfoParserFactory.parseWithTableType(ETableType.SINK.getType(),
                        createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                sqlTree.addTableInfo(tableName, tableInfo);
            }
        }

        return sqlTree;
    }
}
