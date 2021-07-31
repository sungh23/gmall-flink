package com.admin.gmall.realtime.app.ods;

import com.admin.gmall.realtime.app.func.MyStringDebeziumDeserializationSchema;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据流向  mysql（新增及变化数据）-->flinkCDCSource（监控） ---> 发送到kafkaSink
 * @author sungh
 */
public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //构建flinkSource
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall0225_flink")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.print();

        //将数据写入到kafka
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer("ods_base_db"));


        env.execute();
    }
}
