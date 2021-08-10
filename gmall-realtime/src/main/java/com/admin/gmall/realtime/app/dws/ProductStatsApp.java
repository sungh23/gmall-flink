package com.admin.gmall.realtime.app.dws;

import com.admin.gmall.realtime.app.func.DimAsyncFunction;
import com.admin.gmall.realtime.bean.OrderWide;
import com.admin.gmall.realtime.bean.PaymentInfo;
import com.admin.gmall.realtime.bean.PaymentWide;
import com.admin.gmall.realtime.bean.ProductStats;
import com.admin.gmall.realtime.common.GmallConstant;
import com.admin.gmall.realtime.utils.ClickHouseUtil;
import com.admin.gmall.realtime.utils.DateTimeUtil;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author sungh
 *
 * 功    能：商品主题宽表
 * 实现思路：    1、消费kafka数据 （dwd_page_log,dwm_order_wide,dwm_payment_wide
 *              四张维度表数据 dwd_cart_info，dwd_favor_info,dwd_order_refund_info,dwd_comment_info ）
 *              2、将流转换为 ProductStats 类 统一数据格式
 *              2.1 设计orderIdSet集合来保存订单数据 用来对订单数据去重，则Set.size 为去重后订单总数
 *              2.1 设计paidOrderIdSet集合来保存支付订单数据 去重，则Set.size 为去重后支付订单总数
 *              2.1 设计refundOrderIdSet集合来保存退款支付订单数据 去重，则Set.size 为去重后退款订单总数
 *              3、union数据流
 *              4、指定WaterMark 设置延迟时间
 *              5、分组、开窗、聚合 补充窗口开始时间、结束时间、订单  支付订单 退款订单条数（SET、size）
 *              6、关联维度数据  补全维度字段
 *              7、写入clickHouse
 *              8、执行
 *
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启CK
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        //...
//        env.setStateBackend(new FsStateBackend(""));

        //TODO 2.读取Kafka数据
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentWideSourceTopic, groupId));
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(cartInfoSourceTopic, groupId));
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(favorInfoSourceTopic, groupId));
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commonDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(commentInfoSourceTopic, groupId));

        // todo 3.统一数据格式
        // 3.1 pageDS 点击  曝光数据
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pageDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {

                JSONObject jsonObject = JSONObject.parseObject(value);

                Long ts = jsonObject.getLong("ts");
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                String item_type = page.getString("item_type");
                if ("good_detail".equals(page_id) && "sku_id".equals(item_type)) {
                    // 写出一条点击数据  查看商品详情则为点击
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //获取曝光数据集
                JSONArray displays = jsonObject.getJSONArray("displays");

                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        //写出一条曝光数据
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }

            }
        });

        // 3.2  favorDS 收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavoDs = favorDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            return ProductStats.builder()
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.3 cartDS 加入购物车
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.4 orderWideDS 下单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDS.map(line -> {

            OrderWide orderWide = JSONObject.parseObject(line, OrderWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getTotal_amount())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // 3.5 paymentDS 支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = paymentDS.map(line -> {
            PaymentWide paymentWide = JSONObject.parseObject(line, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .paidOrderIdSet(orderIds)
                    .payment_amount(paymentWide.getTotal_amount())
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        // 3.6 退款 refundDS
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {

            JSONObject jsonObject = JSONObject.parseObject(line);
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refundOrderIdSet(orderIds)
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // 3.7 评价 commonDS
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commonDS.map(line -> {
            JSONObject jsonObject = JSONObject.parseObject(line);

            //取出评价类型 1201 1202 1203 1204...
            String appraise = jsonObject.getString("appraise");
            //定义标志位
            long flag = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                flag = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("'sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(flag)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();

        });

        // todo 4.union
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(productStatsWithFavoDs,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        // todo 5生成waterMark  分组 开窗 聚合
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // 5.1 分组 开窗 聚合
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuKeyDS = productStatsWithWMDS.keyBy(ProductStats::getSku_id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        //集合处理
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
//                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
//                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
//                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
//                        System.out.println("stats1" + stats1.getRefund_amount());
//                        System.out.println("stats2" + stats2.getRefund_amount());


                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;

                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {

                        //取出窗口时间
                        long start = window.getStart();
                        long end = window.getEnd();

                        //赋值窗口信息
                        ProductStats productStats = input.iterator().next();
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                        //赋值订单个数
                        productStats.setOrder_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        // 返回数据
                        out.collect(productStats);
                    }
                });


        // TODO 6.关联维度信息
        //6.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuInfoDS = AsyncDataStream.unorderedWait(productStatsWithSkuKeyDS, new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
            @Override
            public String getKey(ProductStats productStats) {
                return productStats.getSku_id().toString();
            }

            @Override
            public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {

                productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                productStats.setTm_id(dimInfo.getLong("TM_ID"));
                productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));

            }
        }, 60, TimeUnit.SECONDS);

        //6.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuInfoDS,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDstream.print("to save>>>>>>>>>");

        //TODO 7.写入ClickHouse
        productStatsWithTmDstream.addSink(ClickHouseUtil.getSink("insert into product_stats_210225 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute();


    }
}
