package com.admin.gmall.realtime.app.dwm;

import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * 功能：统计每日用户访问数量  （需要对用户进行去重）
 * 实现思路：  消费kafka上dwd_page_log数据 --> 转换为json类型数据 --> 按照mid进行keyBy  --> 使用状态编程进行去重
 *            在状态编程种设置状态过期时间为一天，同时设置过期时间的更新策略为OnCreateAndWrite
 *            --> 存入的状态是通过数据中ts 获取到的日期时间 （yyyy-MM-dd）
 *            --> 过滤条件为 last_page_id为null 且状态为null  则为当日首次访问用户
 *            --> 用户首次访问后更新状态
 *            --> 数据写入到kafka  （dwm_unique_visit）
 * @author sungh
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境注意跟Kafka主题分区数保持一致

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend(""));
        // todo 2 读取kafka数据
        String sourceTopic = "dwd_page_log";
        String groupId = "UniqueVisitApp0225";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));
        // todo 3 转换为json类型数据
        SingleOutputStreamOperator<JSONObject> jsonObj = streamSource.map(JSONObject::parseObject);
        // todo 4 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(value -> value.getJSONObject("common").getString("mid"));
        // todo 5 使用状态编程对数据进行去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            //定义状态
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("visit", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                descriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(descriptor);
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //过滤数据
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                if (null == last_page_id) {
                    //查看状态里面有没有数据  主要是去重
                    String visitDate = valueState.value();
                    // 取出时间  （我们是24小时去重，所以只算一天的 ）
                    String curDate = sdf.format(value.getLong("ts"));

                    if (visitDate == null || !visitDate.equals(curDate)) {
                        valueState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        filterDS.print("filterDS:");

        // todo 6 将数据写入到kafka
        filterDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));

        // todo 7 执行任务
        env.execute();


    }
}
