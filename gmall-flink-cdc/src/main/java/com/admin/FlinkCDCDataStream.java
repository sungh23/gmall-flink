package com.admin;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * FlinkCDC 监控数据库变化  将变化数据写入到kafka等中间件中
 * @author sungh
 */
public class FlinkCDCDataStream {
    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","atguigu");

        //1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2、开启CK
        env.enableCheckpointing(5000L);
        // 设置过期时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 同时最多CK个数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置CK 精准一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个ck之间的最小间隔时间
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);
        // 设置重启策略
//        env.setRestartStrategy();
        //设置存储路径
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/210225/ck"));


        //  通过flink cdc 构建source
        DebeziumSourceFunction<String> sourceFun = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall0225_flink")
                .tableList("gmall0225_flink.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> dataStreamSource = env.addSource(sourceFun);

        dataStreamSource.print();

        env.execute();

    }
}
