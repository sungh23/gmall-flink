package com.admin.gmall.realtime.app.dwd;

import com.admin.gmall.realtime.app.func.DimSinkFunction;
import com.admin.gmall.realtime.app.func.TableProcessFunction;
import com.admin.gmall.realtime.bean.TableProcess;
import com.admin.gmall.realtime.app.func.MyStringDebeziumDeserializationSchema;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * 数据流向  kafka（ods层数据）--->kafkaSource（消费）--->配置数据做成广播流--->在hbase中动态添加表
 * 程序      mock  -->flinkCDCApp ---> kafka（zk）---->  BaseDbApp --->hBase/phoenix (hdfs,zk)
 * <p>
 * baseDBApp 功能：从kafka消费ods层数据（主流）、使用flinkCDC监控MYsql中gmall-210225-realtime.table_process配置表数据变化形成配置流（广播流）
 * 使用connect连接两条流，自定义类继承BroadcastProcessFunction 在其中对两条流进行处理
 * 一、处理广播流 过滤字段，判断是否需要在hbase库中建表-->需要  则拼接出建表语句 并执行
 * 二、处理主流   根据广播流中对应字段过滤数据，定义标志位作为分流（kafka 或者 hbase）依据 准备好写入hbase库中表的数据
 *
 * @author sungh
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        // todo 1 执行环境 设置ck
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CK 设置相关参数
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // todo 2 获取数据  主流
        String topic = "ods_base_db";
        String groupId = "BaseDbApp";
        DataStreamSource<String> kafkaOdsBaseDbDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        kafkaOdsBaseDbDS.print("kafkaOdsBaseDbDS:");

        //TODO 3.将每行数据转换为JSONObject         主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaOdsBaseDbDS.map(JSONObject::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //过滤delete操作类型数据
                        return !"delete".equals(value.getString("type"));
                    }
                });

        jsonObjDS.print("jsonObjDS:");
        // todo 4 使用flinkCDC监控Mysql数据库中 配置表变化
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        // todo 5 获取配置流 并将配置流做成广播流广播
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(stateDescriptor);

        // todo 6 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connect = jsonObjDS.connect(broadcastStream);

        // 创建侧输出流Tag
        OutputTag<JSONObject> hBaseTag = new OutputTag<JSONObject>("hbase") {
        };

        // 使用自定义方法处理连接流数据
        SingleOutputStreamOperator<JSONObject> mainDS = connect.process(new TableProcessFunction(stateDescriptor, hBaseTag));

        //打印数据流
        DataStream<JSONObject> hbaseDS = mainDS.getSideOutput(hBaseTag);
        hbaseDS.print("hbaseDS:");
        mainDS.print("mainDS:");

        //写入到对应的sink中
        // 写入到hbase中
        hbaseDS.addSink(new DimSinkFunction());

        mainDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"), element.getString("data").getBytes());
            }
        }));

        env.execute();
    }
}
