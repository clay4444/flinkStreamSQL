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

 

package com.dtstack.flink.sql.source;


import com.dtstack.flink.sql.classloader.DtClassLoader;
import com.dtstack.flink.sql.table.AbsSourceParser;
import com.dtstack.flink.sql.table.SourceTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * streamTableSource
 * Date: 2017/3/10
 * Company: www.dtstack.com
 * @author xuchao
 */

/**
 * 源表之 source 插件的获取factory
 */
public class StreamSourceFactory {

    private static final String CURR_TYPE = "source";

    private static final String DIR_NAME_FORMAT = "%ssource";


    /**
     * @param pluginType  kafka （ 暂时只支持kafka ）
     * @param sqlRootDir  dir
     * @return
     */
    public static AbsSourceParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        // 在 插件dir（plugins）下去获取 对应的插件类型的jar包；
        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);

        //
        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);

        // 获取engine 类型，去掉版本号，  kafka09、 kafka10、kafka11 => kafka
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);

        //com.dtstack.flink.sql.source.kafka.table.KafkaSourceParser
        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);

        // 加载 KafkaSourceParser
        Class<?> sourceParser = dtClassLoader.loadClass(className);

        // 判断 类 AbsSourceParser 和另一个类sourceParser是否相同或是另一个类的超类或接口
        // 也就是说 KafkaSourceParser 一定要 继承 AbsSourceParser
        if(!AbsSourceParser.class.isAssignableFrom(sourceParser)){
            throw new RuntimeException("class " + sourceParser.getName() + " not subClass of AbsSourceParser");
        }

        // 将 sourceParser 类转换成 AbsSourceParser 的一个子类，即 KafkaSourceParser
        return sourceParser.asSubclass(AbsSourceParser.class).newInstance();
    }

    /**
     * The configuration of the type specified data source
     *
     *
     * @param sourceTableInfo
     * @return
     */
    public static Table getStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env,
                                        StreamTableEnvironment tableEnv, String sqlRootDir) throws Exception {

        String sourceTypeStr = sourceTableInfo.getType();
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(sourceTypeStr);    // kafka
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, sourceTypeStr), sqlRootDir);

        //com.dtstack.flink.sql.source.kafka.KafkaSource
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        Class<?> sourceClass = dtClassLoader.loadClass(className);

        if(!IStreamSourceGener.class.isAssignableFrom(sourceClass)){
            throw new RuntimeException("class " + sourceClass.getName() + " not subClass of IStreamSourceGener");
        }

        IStreamSourceGener sourceGener = sourceClass.asSubclass(IStreamSourceGener.class).newInstance();
        Object object = sourceGener.genStreamSource(sourceTableInfo, env, tableEnv);
        return (Table) object;
    }
}
