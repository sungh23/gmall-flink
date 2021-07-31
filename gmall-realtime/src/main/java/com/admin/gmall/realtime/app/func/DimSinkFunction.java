package com.admin.gmall.realtime.app.func;

import com.admin.gmall.realtime.common.GmallConfig;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 自定义类继承RichSinkFunction 实现invoke方法
 * 作用 将数据动态写入到hbase中
 *
 * @author sungh
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    // 定义属性  连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 将流式数据写入到hbase中
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //value:{"database":"","tableName":"base_trademark","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert","sinkTable":"dim_base_trademark"}
        //获取插入数据的SQL upsert into db.tn(id,tm_name) values(..,..)
        PreparedStatement preparedStatement=null;
        try {
            String upsertSql = getUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println("upsertSql = " + upsertSql);

            //预编译
            preparedStatement = connection.prepareStatement(upsertSql);
            //执行
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("插入维度数据" + value.getString("data") + "失败！");
        } finally {
            if (null != preparedStatement) {
                preparedStatement.close();
            }
        }


    }

    //upsert into db.tn(id,tm_name,aa,bb) values('..','..','...','...')
    //data:{"id":"1001","tm_name":"atguigu"}
    private String getUpsertSql(String sinkTable, JSONObject data) {

        // 获取字段
        Set<String> columns = data.keySet();

        // 获取值
        Collection<Object> values = data.values();

        //拼接sql
        return "upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+
                "(" + StringUtils.join(columns,",")+")"+
                "values('"+StringUtils.join(values,"','")+"')";

    }
}
