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

 

package com.dtstack.flink.sql;

import com.dtstack.flink.sql.classloader.DtClassLoader;
import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.parser.CreateFuncParser;
import com.dtstack.flink.sql.parser.InsertSqlParser;
import com.dtstack.flink.sql.side.SideSqlExec;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.table.SourceTableInfo;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import com.dtstack.flink.sql.sink.StreamSinkFactory;
import com.dtstack.flink.sql.source.StreamSourceFactory;
import com.dtstack.flink.sql.watermarker.WaterMarkerAssigner;
import com.dtstack.flink.sql.util.FlinkUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2018/6/26
 * Company: www.dtstack.com
 * @author xuchao
 */

public class Main {

    private static final ObjectMapper objMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final int failureRate = 3;

    private static final int failureInterval = 6; //min

    private static final int delayInterval = 10; //sec

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption("sql", true, "sql config");
        options.addOption("name", true, "job name");
        options.addOption("addjar", true, "add jar");
        options.addOption("localSqlPluginPath", true, "local sql plugin path");
        options.addOption("remoteSqlPluginPath", true, "remote sql plugin path");
        options.addOption("confProp", true, "env properties");
        options.addOption("mode", true, "deploy mode");

        options.addOption("savePointPath", true, "Savepoint restore path");
        options.addOption("allowNonRestoredState", true, "Flag indicating whether non restored state is allowed if the savepoint");

        CommandLineParser parser = new DefaultParser();
        CommandLine cl = parser.parse(options, args);  // 解析具体参数到options 中，


        //具体执行参数，排除掉了 flinkconf 和  yarnconf  两个配置
        String sql = cl.getOptionValue("sql");
        String name = cl.getOptionValue("name");
        String addJarListStr = cl.getOptionValue("addjar");
        String localSqlPluginPath = cl.getOptionValue("localSqlPluginPath");
        String remoteSqlPluginPath = cl.getOptionValue("remoteSqlPluginPath");
        String deployMode = cl.getOptionValue("mode");
        String confProp = cl.getOptionValue("confProp");

        // sql，name，localSqlPluginPath(本地插件打包目录)  三个不能为空
        Preconditions.checkNotNull(sql, "parameters of sql is required");
        Preconditions.checkNotNull(name, "parameters of name is required");
        Preconditions.checkNotNull(localSqlPluginPath, "parameters of localSqlPluginPath is required");

        // 具体执行的sql语句
        sql = URLDecoder.decode(sql, Charsets.UTF_8.name());

        // 本地plugin插件
        SqlParser.setLocalSqlPluginRoot(localSqlPluginPath);

        // UDF jar 包，读到list中，
        List<String> addJarFileList = Lists.newArrayList();
        if(!Strings.isNullOrEmpty(addJarListStr)){
            addJarListStr = URLDecoder.decode(addJarListStr, Charsets.UTF_8.name());
            addJarFileList = objMapper.readValue(addJarListStr, List.class);
        }

        //TODO 使用当前线程的类加载器加载，涉及到 jvm委派链  问题
        ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();
        DtClassLoader dtClassLoader = new DtClassLoader(new URL[]{}, threadClassLoader);
        Thread.currentThread().setContextClassLoader(dtClassLoader);

        URLClassLoader parentClassloader;

        if(!ClusterMode.local.name().equals(deployMode)){  // 本地模式
            parentClassloader = (URLClassLoader) threadClassLoader.getParent();
        }else{
            parentClassloader = dtClassLoader;
        }

        // 配置信息
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());

        // 封装 flink app 启动需要的配置信息  {\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000}
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

        //返回本地env，其中设置了重启策略，checkpoint，并行度，event time 等各种参数，
        StreamExecutionEnvironment env = getStreamExeEnv(confProperties, deployMode);

        // 获取table env
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        List<URL> jarURList = Lists.newArrayList();

        // 获取 sql 语法 树，
        SqlTree sqlTree = SqlParser.parseSql(sql);

        //额外jar包，自定义 udf 等
        for(String addJarPath : addJarFileList){
            File tmpFile = new File(addJarPath);
            jarURList.add(tmpFile.toURI().toURL());
        }

        Map<String, SideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();

        //register udf
        registerUDF(sqlTree, jarURList, parentClassloader, tableEnv);

        //register table schema
        registerTable(sqlTree, env, tableEnv, localSqlPluginPath, remoteSqlPluginPath, sideTableMap, registerTableCache);

        // 维度表执行器
        SideSqlExec sideSqlExec = new SideSqlExec();
        // 设置插件路径
        sideSqlExec.setLocalSqlPluginPath(localSqlPluginPath);

        for (InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()) {
            if(LOG.isInfoEnabled()){
                LOG.info("exe-sql:\n" + result.getExecSql());
            }

            boolean isSide = false;

            for(String tableName : result.getSourceTableList()){
                if(sideTableMap.containsKey(tableName)){
                    isSide = true;
                    break;
                }
            }

            if(isSide){
                //sql-dimensional table contains the dimension table of execution
                // 如果包含维度表，就先执行维度表，registerTableCache 中只包含 source 表，
                sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache);
            }else{
                tableEnv.sqlUpdate(result.getExecSql());
            }
        }

        if(env instanceof MyLocalStreamEnvironment) {
            List<URL> urlList = new ArrayList<>();
            urlList.addAll(Arrays.asList(dtClassLoader.getURLs()));
            ((MyLocalStreamEnvironment) env).setClasspaths(urlList);
        }

        env.execute(name);  // exec
    }

    /**
     * This part is just to add classpath for the jar when reading remote execution, and will not submit jar from a local
     * 这部分只是为了在读取远程执行时添加jar的类路径，并且不会从本地提交jar
     * @param env
     * @param classPathSet
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static void addEnvClassPath(StreamExecutionEnvironment env, Set<URL> classPathSet) throws NoSuchFieldException, IllegalAccessException {
        if(env instanceof StreamContextEnvironment){
            Field field = env.getClass().getDeclaredField("ctx");
            field.setAccessible(true);
            ContextEnvironment contextEnvironment= (ContextEnvironment) field.get(env);
            for(URL url : classPathSet){
                contextEnvironment.getClasspaths().add(url);
            }
        }
    }

    /**
     * 注册  udf
     * @param sqlTree     sql 语法数
     * @param jarURList   jar 包
     * @param parentClassloader
     * @param tableEnv
     */
    private static void registerUDF(SqlTree sqlTree, List<URL> jarURList, URLClassLoader parentClassloader,
                                    StreamTableEnvironment tableEnv)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        //register urf
        URLClassLoader classLoader = null;
        List<CreateFuncParser.SqlParserResult> funcList = sqlTree.getFunctionList();
        for (CreateFuncParser.SqlParserResult funcInfo : funcList) {
            //classloader
            if (classLoader == null) {
                classLoader = FlinkUtil.loadExtraJar(jarURList, parentClassloader);
            }
            classLoader.loadClass(funcInfo.getClassName());
            FlinkUtil.registerUDF(funcInfo.getType(), funcInfo.getClassName(), funcInfo.getName().toUpperCase(),
                    tableEnv, classLoader);
        }
    }


    /**
     * 注册 Table
     * @param sqlTree               sql 语法树，
     * @param env                   env
     * @param tableEnv              tableEnv
     * @param localSqlPluginPath    本地 sql 环境变量
     * @param remoteSqlPluginPath
     * @param sideTableMap
     * @param registerTableCache
     * @throws Exception
     */
    private static void registerTable(SqlTree sqlTree, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv,
                                      String localSqlPluginPath, String remoteSqlPluginPath,
                                      Map<String, SideTableInfo> sideTableMap, Map<String, Table> registerTableCache) throws Exception {
        Set<URL> classPathSet = Sets.newHashSet();
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();

        // 遍历所有的Table
        for (TableInfo tableInfo : sqlTree.getTableInfoMap().values()) {

            if (tableInfo instanceof SourceTableInfo) {
                // Source Table 类型

                SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;

                // 获取Table 环境，例如 kafka，就是把一个kafka stream transform 成一个 Kafka Table
                Table table = StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, localSqlPluginPath);

                // MYTABLE_adapt   Table； 这里注册成一张 adapt 的表名是因为后面的 adaptSql 需要去查询它
                tableEnv.registerTable(sourceTableInfo.getAdaptName(), table);

                //Note --- parameter conversion function can not be used inside a function of the type of polymerization
                //Create table in which the function is arranged only need adaptation sql
                //参数转换函数不能在聚合类型的函数内使用， 去掉watermark属性，其他属性直接拼成一条从源表查询的sql
                // select CHANNEL,PV,XCTIME,CHARACTER_LENGTH(CHANNEL) AS TIMELENG from MYTABLE_adapt
                // 加这一步的步骤，是要把 虚拟属性加上；
                String adaptSql = sourceTableInfo.getAdaptSelectSql();
                Table adaptTable = adaptSql == null ? table : tableEnv.sqlQuery(adaptSql);

                // Table column type info
                RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getTypes(), adaptTable.getSchema().getColumnNames());

                // transform into datastream 转化成流的原因是 需要 handle with watermark
                DataStream adaptStream = tableEnv.toAppendStream(adaptTable, typeInfo);
                String fields = String.join(",", typeInfo.getFieldNames());

                //handle with watermark
                if(waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo)){
                    adaptStream = waterMarkerAssigner.assignWaterMarker(adaptStream, typeInfo, sourceTableInfo.getEventTimeField(), sourceTableInfo.getMaxOutOrderness());
                    fields += ",ROWTIME.ROWTIME";
                }else{
                    fields += ",PROCTIME.PROCTIME";
                }

                // 再转换成真正的表 （ 添加了watermark 的表 ）
                Table regTable = tableEnv.fromDataStream(adaptStream, fields);

                // 注册真正的表
                tableEnv.registerTable(tableInfo.getName(), regTable);

                // Table 缓存
                registerTableCache.put(tableInfo.getName(), regTable);

                // url 集合
                classPathSet.add(PluginUtil.getRemoteJarFilePath(tableInfo.getType(), SourceTableInfo.SOURCE_SUFFIX, remoteSqlPluginPath));
            } else if (tableInfo instanceof TargetTableInfo) {
                // Target Table 类型

                // 获取一种实现了 RetractStreamTableSink 的撤回流的 Table Sink
                TableSink tableSink = StreamSinkFactory.getTableSink((TargetTableInfo) tableInfo, localSqlPluginPath);
                TypeInformation[] flinkTypes = FlinkUtil.transformTypes(tableInfo.getFieldClasses());

                // env 中注册 table sink
                tableEnv.registerTableSink(tableInfo.getName(), tableInfo.getFields(), flinkTypes, tableSink);
                classPathSet.add( PluginUtil.getRemoteJarFilePath(tableInfo.getType(), TargetTableInfo.TARGET_SUFFIX, remoteSqlPluginPath));
            } else if(tableInfo instanceof SideTableInfo){
                // 维表

                String sideOperator = ECacheType.ALL.name().equals(((SideTableInfo) tableInfo).getCacheType()) ? "all" : "async";
                sideTableMap.put(tableInfo.getName(), (SideTableInfo) tableInfo);
                classPathSet.add(PluginUtil.getRemoteSideJarFilePath(tableInfo.getType(), sideOperator, SideTableInfo.TARGET_SUFFIX, remoteSqlPluginPath));
            }else {
                throw new RuntimeException("not support table type:" + tableInfo.getType());
            }
        }

        //The plug-in information corresponding to the table is loaded into the classPath env
        addEnvClassPath(env, classPathSet);
    }

    //
    private static StreamExecutionEnvironment getStreamExeEnv(Properties confProperties, String deployMode) throws IOException {
        //
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(deployMode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();  // local 模拟下，

        //仅能设置sql 的并行度，
        env.setParallelism(FlinkUtil.getEnvParallelism(confProperties));

        // sql 环境 最大并行度
        if(FlinkUtil.getMaxEnvParallelism(confProperties) > 0){
            env.setMaxParallelism(FlinkUtil.getMaxEnvParallelism(confProperties));
        }

        // sql.buffer 超时时间
        if(FlinkUtil.getBufferTimeoutMillis(confProperties) > 0){
            env.setBufferTimeout(FlinkUtil.getBufferTimeoutMillis(confProperties));
        }

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                failureRate, //给定间隔的最大重试次数
                Time.of(failureInterval, TimeUnit.MINUTES), // 时间间隔
                Time.of(delayInterval, TimeUnit.SECONDS)  //每次重启之间，要延迟的时间，
        ));

        FlinkUtil.setStreamTimeCharacteristic(env, confProperties);
        FlinkUtil.openCheckpoint(env, confProperties);

        return env;
    }
}
