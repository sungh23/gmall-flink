package com.admin.gmall.realtime.app.dwm;

import com.admin.gmall.realtime.bean.OrderWide;
import com.admin.gmall.realtime.bean.PaymentInfo;
import com.admin.gmall.realtime.bean.PaymentWide;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 数据流:web/app -> nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ods) -> FlinkApp -> Kafka/Phoenix(dwd/dim) -> FlinkApp -> Kafka(dwm)
 * 程  序:       mockDb -> Mysql -> FlinkCDCApp -> Kafka(ZK) -> BaseDbApp -> Kafka/Phoenix -> PaymentWideApp(OrderWideApp) -> Kafka
 *
 * @author sungh
 */
public class PaymentWideApp {
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

        //TODO 2.读取Kafka两个主题的数据
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentInfoDSKf = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDSKf = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));

        // todo 3 将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoDSKf.map(line -> JSONObject.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideDSKf.map(line -> JSONObject.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        // todo 4 keyBy 之后再进行 双流join
        SingleOutputStreamOperator<PaymentWide> process = orderWideDS.keyBy(OrderWide::getOrder_id)
                .intervalJoin(paymentInfoDS.keyBy(PaymentInfo::getOrder_id))
                .between(Time.minutes(0), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        process.print("process:");

        // todo 5 将数据写入到kafka
        process.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));

        env.execute();

    }
}
