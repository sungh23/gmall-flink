package com.admin.gmall.realtime.app.dwm;

import com.admin.gmall.realtime.app.func.DimAsyncFunction;
import com.admin.gmall.realtime.bean.OrderDetail;
import com.admin.gmall.realtime.bean.OrderInfo;
import com.admin.gmall.realtime.bean.OrderWide;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.mysql.jdbc.TimeUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * 功能 订单宽表
 *
 * 实现思路  1、读取kafka（dwd_order_info、dwd_order_detail）数据获取到order_info 和 order_detail的两条流
 *          2、将订单和订单明细两张事实表做双流join 之后用实体类OrderWide接收形成新的数据流
 *          3、关联维度数据
 *          3.1 使用自定义DimAsyncFunction类实现异步方法 实现异步查询phoenix关联六大维度表
 *              六大维度表：用户表（DIM_USER_INFO） ，地区表(DIM_BASE_PROVINCE) ， 商品表（DIM_SKU_INFO），spu表(DIM_SPU_INFO) ，分类表 (DIM_BASE_CATEGORY3) ，品牌表 (DIM_BASE_TRADEMARK)
 *
 * 数据流 web/app -- nginx --springboot -- mysql -- flinkApp -- kafka(ods)        -- flinkApp -- kafka(dwd) -- flinkApp -- kafka(dwm)
 * 程序              mock               -- mysql -- flinkCDC -- kafka(ods_base_db)--BaseDbApp-- phoenix/kafka -- orderWideApp --kafka
 * @author sungh
 */
public class OrderWideApp {
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

        //TODO 2.读取Kafka dwd_order_info dwd_order_detail主题数据
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group1";

        DataStreamSource<String> orderInfoDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoWDS = orderInfoDS.map(new MapFunction<String, OrderInfo>() {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public OrderInfo map(String value) throws Exception {

                OrderInfo orderInfo = JSONObject.parseObject(value, OrderInfo.class);
                //处理子段
                String[] split = orderInfo.getCreate_time().split(" ");
                orderInfo.setCreate_date(split[0]);
                orderInfo.setCreate_hour(split[1].split(":")[0]);
                orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());

                return orderInfo;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));


        SingleOutputStreamOperator<OrderDetail> orderDetailWDS = orderDetailDS.map(line -> {
            OrderDetail orderDetail = JSONObject.parseObject(line, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());

            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }
        ));

        // todo 4 双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });


        // todo 5 关联维度数据
        // todo 5.1关联用户维度  DIM_USER_INFO
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getUser_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {
                String gender = dimInfo.getString("GENDER");
                orderWide.setUser_gender(gender);

                String birthday = dimInfo.getString("BIRTHDAY");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    long curT = System.currentTimeMillis();
                    long ts = sdf.parse(birthday).getTime();
                    long age = (curT - ts) / (1000L * 60 * 60 * 24 * 365);
                    orderWide.setUser_age((int) age);

                } catch (ParseException e) {
                    e.printStackTrace();
                    System.out.println("日期格式转换异常");
                }

            }
        }, 60, TimeUnit.SECONDS);

        // todo 5.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide orderWide) {
                return orderWide.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) {

                String name = dimInfo.getString("NAME");
                String area_code = dimInfo.getString("AREA_CODE");
                String iso_code = dimInfo.getString("ISO_CODE");
                String province_3166_2_code = dimInfo.getString("ISO_3166_2");
                orderWide.setProvince_name(name);
                orderWide.setProvince_area_code(area_code);
                orderWide.setProvince_iso_code(iso_code);
                orderWide.setProvince_3166_2_code(province_3166_2_code);

            }
        }, 60, TimeUnit.SECONDS);

        //5.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //5.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("Result>>>>>>>>>>");

//        // todo 6 将数据写入到kafka
        orderWideWithCategory3DS.map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(orderWideSinkTopic));

        env.execute();

    }
}
