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

 

package com.dtstack.flink.sql.sink.mysql;


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.mysql.table.MysqlTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Date: 2017/2/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlSink extends DBSink implements IStreamSinkGener<MysqlSink> {

    public MysqlSink(){
    }

    // 创建sql
    @Override
    public void buildSql(String tableName, List<String> fields){
        buildInsertSql(tableName, fields);
    }

    /**
     * 构建插入操作的sql语句
     * replace into ... (..) values (...)
     */
    private void buildInsertSql(String tableName, List<String> fields){
        String sqlTmp = "replace into " + tableName + " (${fields}) values (${placeholder})";
        String fieldsStr = "";
        String placeholder = "";

        for(String fieldName : fields){
            fieldsStr += ",`" + fieldName + "`";
            placeholder += ",?";
        }

        fieldsStr = fieldsStr.replaceFirst(",", "");
        placeholder = placeholder.replaceFirst(",", "");

        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
    }


    // 获取mysql sink
    @Override
    public MysqlSink genStreamSink(TargetTableInfo targetTableInfo) {

        MysqlTableInfo mysqlTableInfo = (MysqlTableInfo) targetTableInfo;

        String tmpDbURL = mysqlTableInfo.getUrl();
        String tmpUserName = mysqlTableInfo.getUserName();
        String tmpPassword = mysqlTableInfo.getPassword();
        String tmpTableName = mysqlTableInfo.getTableName();

        Integer tmpSqlBatchSize = mysqlTableInfo.getBatchSize();
        if(tmpSqlBatchSize != null){
            setBatchInterval(tmpSqlBatchSize);
        }

        Integer tmpSinkParallelism = mysqlTableInfo.getParallelism();
        if(tmpSinkParallelism != null){
            setParallelism(tmpSinkParallelism);
        }

        List<String> fields = Arrays.asList(mysqlTableInfo.getFields());
        List<Class> fieldTypeArray = Arrays.asList(mysqlTableInfo.getFieldClasses());

        this.driverName = "com.mysql.jdbc.Driver";
        this.dbURL = tmpDbURL;
        this.userName = tmpUserName;
        this.password = tmpPassword;
        this.tableName = tmpTableName;
        this.primaryKeys = mysqlTableInfo.getPrimaryKeys();
        //构建sql 语句
        buildSql(tableName, fields);
        // 设置 sqlTypes，（jdbc 定义的）
        buildSqlTypes(fieldTypeArray);
        return this;
    }

}
