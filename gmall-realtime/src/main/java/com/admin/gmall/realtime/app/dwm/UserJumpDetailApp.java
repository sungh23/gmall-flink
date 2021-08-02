package com.admin.gmall.realtime.app.dwm;

import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 功能：统计用户页面跳出率  跳出率=跳出次数/访问次数
 * 跳出：用户进来之后（一次会话）只访问了一个页面就直接退出APP
 * 实现思路：  从kafka消费数据（dwd_page_log） --> 转换为json类型数据 同时定义waterMark --> 根据mid进行keyBy
 *            --> 定义CEP模式  严格连续
 *            --> 过滤条件一  last_page_id 为null
 *            --> 过滤条件二  last_page_id 为null  （只有连续两次为null 才能判断用户上次一次会话已经结束，为跳出操作）
 *            --> 定义超时时间  考虑到用户可能在规定的时间内只访问一次 直接跳出的情况
 *            --> 将模式作用在流上  获取
 *            --> 提取事件 包括匹配事件和超时事件
 *            --> 使用侧输出流将超时事件输出
 *            --> 将主流和侧输出流进行union
 *            --> 发送到kafka （dwm_user_jump_detail）
 *
 *
 *
 * @author sungh
 */
public class UserJumpDetailApp {
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

        //TODO 2.读取Kafka dwd_page_log 主题数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp0225";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        // todo 3 转换为json数据 定义warteMaek 并分组
        KeyedStream<JSONObject, String> keyedStream = kafkaDS.map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })).keyBy(r -> r.getJSONObject("common").getString("mid"));

        // todo 4 定义cep 模式  严格连续
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }).next("end")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                    //定义超时输出 是考虑到有的用户只会访问一次  后面一直没有访问  这样也算一次跳出
                }).within(Time.seconds(10));

        // todo 4.1 定义cep 模式  严格连续
        Pattern<JSONObject, JSONObject> pattern2 = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(10));

        // todo 5 将模式作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // todo 6 提取事件(包含匹配上的以及超时事件)
        OutputTag<JSONObject> timeTag = new OutputTag<JSONObject>("time"){};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        // todo 7 将侧输出流和主流进行union
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeTag);
        timeOutDS.print("timeOutDS");
        DataStream<JSONObject> dataStream = selectDS.union(timeOutDS);

        dataStream.print("dataStream:");
        // todo 8 将数据写入到kafka
        dataStream.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));
        // todo 9 执行
        env.execute();


    }
}
