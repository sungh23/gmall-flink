package com.admin.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

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
