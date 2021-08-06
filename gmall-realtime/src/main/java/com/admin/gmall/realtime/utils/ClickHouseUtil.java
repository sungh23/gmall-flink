package com.admin.gmall.realtime.utils;

import com.admin.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import javax.management.monitor.GaugeMonitor;
import java.beans.Transient;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author sungh
 */
public class ClickHouseUtil {

//    public static <T> SinkFunction<T> getSink(String sql) {
//
//        return JdbcSink.<T>sink(sql, new JdbcStatementBuilder<T>() {
//            @Override
//            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
//
//                //获取所有属性名称
//                Field[] fields = t.getClass().getDeclaredFields();
//
//                //遍历属性名字 获取数据内容并给preparedStatement赋值
//                for (int i = 0; i < fields.length; i++) {
//
//                    //获取字段名
//                    Field field = fields[i];
//
//                    //设置私有属性可访问
//                    field.setAccessible(true);
//
//                    //获取字段注解
//                    Transient annotation = field.getAnnotation(Transient.class);
//
//                    if (annotation!=null){
//
//                    }
//
//                    //获取字段值
//                    try {
//                        Object value = field.get(t);
//
//                        preparedStatement.setObject(i+1,value);
//
//                    } catch (IllegalAccessException e) {
//                        e.printStackTrace();
//                    }
//
//
//                }
//
//
//            }
//        }, )
//
//    }

    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //获取所有属性名称
                        Field[] declaredFields = t.getClass().getDeclaredFields();


                        //遍历 获取数据内容并给preparedStatement赋值
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {


                            Field declaredField = declaredFields[i];

                            // 设置私有属性都能访问
                            declaredField.setAccessible(true);

                            // 获取注解  有注解字段则忽略
                            Transient annotation = declaredField.getAnnotation(Transient.class);

                            if (annotation == null) {

                                try {
                                    Object value = declaredField.get(t);
                                    preparedStatement.setObject(i + 1 - offset, value);
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                }

                            } else {
                                offset++;
                            }
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );

    }
}
