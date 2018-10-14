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


import com.dtstack.flink.sql.table.TableInfo;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * 解析全部 sql 获得的对象结构
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SqlTree {   // sql 获得的对象结构

    // CreateFuncParser 解析创建函数后  的信息保存在这个list，包含创建的函数名、函数类型、class全类名
    private List<CreateFuncParser.SqlParserResult> functionList = Lists.newArrayList();

    // createTableParser 解析创建源表后 的信息保存在这个map ，包含创建的源表名、属性名和类型、with补充信息
    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = Maps.newHashMap();

    /**
     * <TableName, TableInfo>  前面三种固定的 parser 解析完之后，后添加的，
     * TableInfo 中封装了表名，表属性，表类型，Class，主键，等
     */
    private Map<String, TableInfo> tableInfoMap = Maps.newLinkedHashMap();

    // insertTableParser 解析用户执行sql后 的信息保存在这个list，包含源表名、目标表名、用户原始sql
    private List<InsertSqlParser.SqlParseResult> execSqlList = Lists.newArrayList();

    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }


    //添加 CreateFuncParser 解析后的信息
    public void addFunc(CreateFuncParser.SqlParserResult func){
        functionList.add(func);
    }


    //添加 createTableParser 解析后的信息
    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table){
        preDealTableMap.put(tableName, table);
    }

    //添加 insertTableParser 解析后的信息
    public void addExecSql(InsertSqlParser.SqlParseResult execSql){
        execSqlList.add(execSql);
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public void addTableInfo(String tableName, TableInfo tableInfo){
        tableInfoMap.put(tableName, tableInfo);
    }
}
