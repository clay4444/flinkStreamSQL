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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * 解析flink sql
 * sql 只支持 insert 开头的
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class InsertSqlParser implements IParser {

    // insert 开头
    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance(){
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    //解析sql
    @Override
    public void parseSql(String sql, SqlTree sqlTree) {

        // calcite 提供的解析 insert table select * from startTable join side table 的解析器，
        SqlParser sqlParser = SqlParser.create(sql);
        // SqlNode：链表，
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();  // 解析sql，获取sqlNode ，然后递归解析
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }

        // 包含原始表，结果表，和执行sql
        SqlParseResult sqlParseResult = new SqlParseResult();

        parseNode(sqlNode, sqlParseResult);
        sqlParseResult.setExecSql(sqlNode.toString()); //insert into xxx select ** 的原始大sql
        sqlTree.addExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT: // insert 类型的sql，
                SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();  // MyResult
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();// SELECT `A`.`CHANNEL`, `A`.`PV`, `B`.`XCCOUNT`, `B`.`XCAGE` FROM `MYTABLE` AS `A` INNER JOIN `SIDETABLE` AS `B` ON `A`.`CHANNEL` = `B`.`CHANNEL` WHERE `B`.`CHANNEL` = 'xc'
                // 获取 target Table: insert into 后面的，
                sqlParseResult.addTargetTable(sqlTarget.toString());
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();  // from 后面的
                if(sqlFrom.getKind() == IDENTIFIER){   //如果没有join，那么这一步就返回了true，那from后面的就是源表，
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                }else{
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft(); //leftNode  就是源表 + as 别名
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();  // rightNode 就是维度表 + as 别名

                if(leftNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else{
                    parseNode(leftNode, sqlParseResult);
                }

                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];  // 返回源表名 ( 去掉as )
                if(identifierNode.getKind() != IDENTIFIER){  // 那 IDENTIFIER (鉴定) 其实就是判断表名后面有没有as，
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    // 获取 source Table: 也就是  from 后面的第一张表（还要去掉as）
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            default:
                //do nothing
                break;
        }
    }

    public static class SqlParseResult {

        private List<String> sourceTableList = Lists.newArrayList();

        private List<String> targetTableList = Lists.newArrayList();

        private String execSql;

        public void addSourceTable(String sourceTable){
            sourceTableList.add(sourceTable);
        }

        public void addTargetTable(String targetTable){
            targetTableList.add(targetTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }

        public List<String> getTargetTableList() {
            return targetTableList;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }
    }
}
