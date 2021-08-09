package com.admin.gmall.realtime.app.dws;

import com.admin.gmall.realtime.bean.ProvinceStats;
import com.admin.gmall.realtime.utils.ClickHouseUtil;
import com.admin.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sungh
 */
public class ProvinceStatsSqlApp {
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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // todo 2.使用DDL的方式读取kafka dwd_order_wide主题数据 并设置WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE order_wide (" +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `total_amount` DOUBLE, " +
                "  `create_time` STRING, " +
                "  `rt` AS TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " + //定义watermark
                ")"+ MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId));

        // todo 3 分组 开窗 聚合
        Table sqlQuery = tableEnv.sqlQuery("select  " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +  //窗口开始时间
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +  //窗口结束时间
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    sum(total_amount) order_amount,  " +
                "    count(distinct order_id) order_count,  " +
                "    UNIX_TIMESTAMP() AS ts " +
                "from order_wide  " +
                "group by  " +
                "    province_id,  " +
                "    province_name,  " +
                "    province_area_code,  " +
                "    province_iso_code,  " +
                "    province_3166_2_code,  " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)"); //开启滚动窗口

        // todo 4将动态表转换为数据流
        DataStream<ProvinceStats> resultDS = tableEnv.toAppendStream(sqlQuery, ProvinceStats.class);

        // todo 5 将数据写入到clickHouse
        resultDS.addSink(ClickHouseUtil.getSink("insert into province_stats_210225 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();

    }
}
