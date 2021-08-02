package com.admin.gmall.realtime.app.func;

import com.admin.gmall.realtime.common.GmallConfig;
import com.admin.gmall.realtime.utils.DimUtil;
import com.admin.gmall.realtime.utils.ThreadPoolUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.kafka.common.utils.ThreadUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author sungh
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    // 定义属性 线程池  phoenix连接
    private ThreadPoolExecutor threadPoolExecutor;
    private Connection connection;

    //定义属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池 和连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection=DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor= ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        // 使用多线程提交一个任务执行
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //使用抽象方法的方式将参数传入
                String id = getKey(input);

                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);

                if (dimInfo != null) {
                    join(input,dimInfo);
                }
                resultFuture.complete(Collections.singletonList(input));
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("timeOut = " + input);
    }

}
