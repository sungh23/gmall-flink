package com.admin.gmall.realtime.app.dws;

import com.admin.gmall.realtime.bean.VisitorStats;
import com.admin.gmall.realtime.utils.DateTimeUtil;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @author sungh
 */
public class VisitorStatsApp {
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

        //TODO 2.读取Kafka 3个主题的数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));

        // TODO 3 转换为javaBean 并补全字段

        // 3.1 处理pageView数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithPvDS = pageDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });

        // 3.2 处理uvDS 数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        // 3.3 处理uj数据
        SingleOutputStreamOperator<VisitorStats> visitStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts")
            );
        });

        //todo 使用union将数据聚合在一起
        DataStream<VisitorStats> union = visitStatsWithUvDS.union(visitStatsWithPvDS, visitStatsWithUjDS);

        // todo 设置waterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWM = union.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        //todo 进行keyBy操作
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWM.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(value.getAr(), value.getCh(), value.getVc(), value.getIs_new());
            }
        });


        // 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //聚合
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {

                value1.setPv_ct(value2.getPv_ct() + value1.getPv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                VisitorStats visitorStats = input.iterator().next();
                //补充 窗口开始时间和结束时间
                String sst = DateTimeUtil.toYMDhms(new Date(window.getStart()));
                String edt = DateTimeUtil.toYMDhms(new Date(window.getEnd()));
                visitorStats.setStt(sst);
                visitorStats.setEdt(edt);

                out.collect(visitorStats);
            }
        });

        result.print();

        env.execute();


    }
}
