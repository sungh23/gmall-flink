package com.admin.gmall.realtime.app.dwd;

import com.admin.gmall.realtime.utils.MyKafkaUtil;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.security.Principal;

/**
 * @author sungh
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //todo 1 获取kafka中ods_base_log 数据
        String topic = "ods_base_log";
        String groupId = "BaseLogApp";
        DataStreamSource<String> odsLogDStream = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        // todo 2 转换为json类型数据 同时过滤脏数据  （使用侧输出流将脏数据输出）
        OutputTag<String> DirtyData = new OutputTag<String>("DirtyData") {};
        SingleOutputStreamOperator<JSONObject> jsonObj = odsLogDStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //转换异常 ==》脏数据 使用侧输出流输出
                    ctx.output(DirtyData, value);
                }
            }
        });

        //打印出脏数据
        jsonObj.getSideOutput(DirtyData).print("DirtyData: ");

        // todo 3 根据mid进行分组
        KeyedStream<JSONObject, Object> keyedStream = jsonObj.keyBy(json -> json.getJSONObject("common").get("mid"));
        // todo 4 新老用户校验(使用状态编程)
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            //定义状态
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("flag-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取is_new字段 判断
                String isNew = value.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    String state = valueState.value();
                    //判断状态是否存在
                    if (null != state) {
                        // 存在  则为假冒第一次  将is_new置为0
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //不存在  首次 -- 修改存入状态
                        valueState.update("0");
                    }
                }
                return value;
            }
        });

        // todo 5 使用ce输出流将数据分流输出  主流-->page 启动--start 曝光--dispaly
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<JSONObject> processDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject start = value.getJSONObject("start");
                if (null != start) {
                    ctx.output(startTag, start.toJSONString());
                } else {

                    //输出到主流
                    out.collect(value);

                    JSONArray displays = value.getJSONArray("displays");

                    if (null !=displays && displays.size()>0) {
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("pageID", pageId);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                }

            }
        });


        //获取所有的流  并把数据发送到kafka上
        DataStream<String> startOut = processDS.getSideOutput(startTag);
        DataStream<String> displayOut = processDS.getSideOutput(displayTag);

        String startTopic = "dwd_start_log";
        String pageTopic = "dwd_page_log";
        String displayTopic = "dwd_display_log";

        startOut.print("start:");
        displayOut.print("dispaly:");
        processDS.print("page:");

        startOut.addSink(MyKafkaUtil.getFlinkKafkaProducer(startTopic));
        displayOut.addSink(MyKafkaUtil.getFlinkKafkaProducer(displayTopic));
        processDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(pageTopic));


        env.execute();
    }
}
