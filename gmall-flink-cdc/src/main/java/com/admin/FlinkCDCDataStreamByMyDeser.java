package com.admin;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * 自定义序列化器
 *
 * @author sungh
 */
public class FlinkCDCDataStreamByMyDeser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 构建flinkCDC source
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.latest())
                .databaseList("gmall0225_flink")
                .tableList("gmall0225_flink.base_trademark")
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();

        env.execute();
    }

    public static class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //解析出需要的数据  转换成json类型数据返回
            //{
            // "database":"",
            // "tableName":"",
            // "data":{"id":"1001","tm_name","atguigu"....},
            // "before":{"id":"1001","tm_name","atguigu"....},
            // "type":"update",
            // "ts":141564651515
            // }
            //创建json对象
            JSONObject result = new JSONObject();

            //获取库名  表名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String dataBase = split[1];
            String tableName = split[2];

            //获取数据
            Struct value = (Struct) sourceRecord.value();
            JSONObject data = new JSONObject();
            Struct after = value.getStruct("after");
            // 判空  delete类型的after为空
            if (null != after) {
                List<Field> fieldList = after.schema().fields();
                for (Field field : fieldList) {
                    data.put(field.name(), after.get(field));
                }
            }

            Struct before = value.getStruct("before");
            JSONObject beforeData = new JSONObject();
            if (null != before) {
                List<Field> fields = before.schema().fields();
                for (Field field : fields) {
                    beforeData.put(field.name(), before.get(field));
                }
            }

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }

            //封装数据
            result.put("database",dataBase);
            result.put("tableName",tableName);
            result.put("data",data);
            result.put("before",beforeData);
            result.put("type",type);

            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
