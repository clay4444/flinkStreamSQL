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

 

package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Queues;

import java.util.Queue;
import java.util.Set;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * Parsing sql, obtain execution information dimension table
 * Date: 2018/7/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class SideSQLParser {

    // 获取执行队列
    public Queue<Object> getExeQueue(String exeSql, Set<String> sideTableSet) throws SqlParseException {
        exeSql = DtStringUtil.replaceIgnoreQuota(exeSql, "`", "");
        System.out.println("---exeSql---");
        System.out.println(exeSql);

        //TODO
        Queue<Object> queueInfo = Queues.newLinkedBlockingQueue();

        // calcite sql 解析器
        SqlParser sqlParser = SqlParser.create(exeSql);
        SqlNode sqlNode = sqlParser.parseStmt();

        //解析sqlNode
        parseSql(sqlNode, sideTableSet, queueInfo);

        queueInfo.offer(sqlNode);   // 用户的原始的sql 语句；INSERT INTO `MYRESULT` (SELECT `A`.`CHANNEL`, `A`.`PV`, `B`.`XCCOUNT`, `B`.`XCAGE` FROM `MYTABLE_SIDETABLE` AS `A_B` WHERE `B`.`CHANNEL` = 'xc')
        return queueInfo;  // 第一个元素：joinInfo ，第二个元素：用户sql语句
    }

    //解析sql
    private Object parseSql(SqlNode sqlNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();// SELECT `A`.`CHANNEL`, `A`.`PV`, `B`.`XCCOUNT`, `B`.`XCAGE` FROM `MYTABLE` AS `A` INNER JOIN `SIDETABLE` AS `B` ON `A`.`CHANNEL` = `B`.`CHANNEL` WHERE `B`.`CHANNEL` = 'xc'
                return parseSql(sqlSource, sideTableSet, queueInfo);
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();  // from 后面的
                if(sqlFrom.getKind() != IDENTIFIER){   // join
                    Object result = parseSql(sqlFrom, sideTableSet, queueInfo);
                    if(result instanceof JoinInfo){
                        // join 信息
                        //
                        dealSelectResultWithJoinInfo((JoinInfo)result, (SqlSelect) sqlNode, queueInfo);
                    }else if(result instanceof AliasInfo){
                        String tableName = ((AliasInfo) result).getName();
                        if(sideTableSet.contains(tableName)){
                            throw new RuntimeException("side-table must be used in join operator");
                        }
                    }
                }else{  // 非join，后面没有其他东西了
                    String tableName = ((SqlIdentifier)sqlFrom).getSimple();
                    if(sideTableSet.contains(tableName)){
                        throw new RuntimeException("side-table must be used in join operator");
                    }
                }
                break;
            case JOIN:   // 这里直接返回了
                return dealJoinNode((SqlJoin) sqlNode, sideTableSet, queueInfo);
            case AS:
                SqlNode info = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                String infoStr;

                if(info.getKind() == IDENTIFIER){
                    infoStr = info.toString();
                }else{
                    infoStr = parseSql(info, sideTableSet, queueInfo).toString();
                }

                AliasInfo aliasInfo = new AliasInfo();
                aliasInfo.setName(infoStr);
                aliasInfo.setAlias(alias.toString());

                return aliasInfo;
        }

        return "";
    }

    /**
     * 处理 join 的情况
     * @return JoinInfo(sql 语句的情况，左表，右表，左链接，右连接，别名等信息)
     */
    private JoinInfo dealJoinNode(SqlJoin joinNode, Set<String> sideTableSet, Queue<Object> queueInfo){
        SqlNode leftNode = joinNode.getLeft();
        SqlNode rightNode = joinNode.getRight();
        JoinType joinType = joinNode.getJoinType();  // join 类型

        // 处理左边，
        String leftTbName = "";
        String leftTbAlias = "";

        if(leftNode.getKind() == IDENTIFIER){  //`MYTABLE` AS `A`
            leftTbName = leftNode.toString();
        }else if(leftNode.getKind() == JOIN){
            Object leftNodeJoinInfo = parseSql(leftNode, sideTableSet, queueInfo);
            System.out.println(leftNodeJoinInfo);
        }else if(leftNode.getKind() == AS){
            AliasInfo aliasInfo = (AliasInfo) parseSql(leftNode, sideTableSet, queueInfo);
            leftTbName = aliasInfo.getName();
            leftTbAlias = aliasInfo.getAlias();
        }else{
            throw new RuntimeException("---not deal---");
        }

        // 维度表必须在右边（小表驱动大表）
        boolean leftIsSide = checkIsSideTable(leftTbName, sideTableSet);
        if(leftIsSide){
            throw new RuntimeException("side-table must be at the right of join operator");
        }

        // 处理右表
        String rightTableName = "";
        String rightTableAlias = "";

        if(rightNode.getKind() == IDENTIFIER){
            rightTableName = rightNode.toString();
        }else{
            AliasInfo aliasInfo = (AliasInfo)parseSql(rightNode, sideTableSet, queueInfo);
            rightTableName = aliasInfo.getName();
            rightTableAlias = aliasInfo.getAlias();
        }

        boolean rightIsSide = checkIsSideTable(rightTableName, sideTableSet);

        //不支持右连接
        if(joinType == JoinType.RIGHT){
            throw new RuntimeException("side join not support join type of right[current support inner join and left join]");
        }

        JoinInfo tableInfo = new JoinInfo();
        tableInfo.setLeftTableName(leftTbName);
        tableInfo.setRightTableName(rightTableName);
        tableInfo.setLeftTableAlias(leftTbAlias);
        tableInfo.setRightTableAlias(rightTableAlias);
        tableInfo.setLeftIsSideTable(leftIsSide);   //false
        tableInfo.setRightIsSideTable(rightIsSide);  //true
        tableInfo.setLeftNode(leftNode);   //`MYTABLE` AS `A`
        tableInfo.setRightNode(rightNode);   // `SIDETABLE` AS `B`
        tableInfo.setJoinType(joinType);
        tableInfo.setCondition(joinNode.getCondition());

        return tableInfo;
    }


    /**
     * 处理 查询结果
     * @param joinInfo
     * @param sqlNode    SELECT `A`.`CHANNEL`, `A`.`PV`, `B`.`XCCOUNT`, `B`.`XCAGE` FROM `MYTABLE` AS `A` INNER JOIN `SIDETABLE` AS `B` ON `A`.`CHANNEL` = `B`.`CHANNEL` WHERE `B`.`CHANNEL` = 'xc'
     * @param queueInfo
     */
    private void dealSelectResultWithJoinInfo(JoinInfo joinInfo, SqlSelect sqlNode, Queue<Object> queueInfo){
        //SideJoinInfo rename
        if(joinInfo.checkIsSide()){  // 肯定是join的

            joinInfo.setSelectFields(sqlNode.getSelectList()); // `A`.`CHANNEL`, `A`.`PV`, `B`.`XCCOUNT`, `B`.`XCAGE`
            joinInfo.setSelectNode(sqlNode);
            if(joinInfo.isRightIsSideTable()){  //维度表肯定在右边
                //Analyzing left is not a simple table
                if(joinInfo.getLeftNode().toString().contains("SELECT")){   // 左表还是个查询表；
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);   // 放入队列
            }else{
                //Determining right is not a simple table
                if(joinInfo.getRightNode().getKind() == SELECT){
                    queueInfo.offer(joinInfo.getLeftNode());
                }

                queueInfo.offer(joinInfo);
            }

            //Update from node

            // SqlOperator 是sql解析树中的一种节点，而不是一个节点，它包括函数，运算符，如'='，和句法结构，如'case'语句
            SqlOperator operator = new SqlAsOperator();

            // SqlParserPos表示SQL语句文本中已解析标记的位置
            SqlParserPos sqlParserPos = new SqlParserPos(0, 0);

            String joinLeftTableName = joinInfo.getLeftTableName();  // 左表名，
            String joinLeftTableAlias = joinInfo.getLeftTableAlias();  // 左表别名，

            joinLeftTableName = Strings.isNullOrEmpty(joinLeftTableName) ? joinLeftTableAlias : joinLeftTableName;

            //新表名 = 左表名_右表名
            String newTableName = joinLeftTableName + "_" + joinInfo.getRightTableName();
            //新别名 = 左表别名_右表别名
            String newTableAlias = joinInfo.getLeftTableAlias() + "_" + joinInfo.getRightTableAlias();

            //SqlIdentifier   标识符，可能是复合词
            SqlIdentifier sqlIdentifier = new SqlIdentifier(newTableName, null, sqlParserPos);
            SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(newTableAlias, null, sqlParserPos);

            // 使用新表名和新别名构造一个SqlNode数组
            SqlNode[] sqlNodes = new SqlNode[2];
            sqlNodes[0] = sqlIdentifier;
            sqlNodes[1] = sqlIdentifierAlias;

            SqlBasicCall sqlBasicCall = new SqlBasicCall(operator, sqlNodes, sqlParserPos);
            sqlNode.setFrom(sqlBasicCall);
        }
    }

    private boolean checkIsSideTable(String tableName, Set<String> sideTableList){
        if(sideTableList.contains(tableName)){
            return true;
        }

        return false;
    }
}
