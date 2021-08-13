package com.admin.gmall.realtime.app.func;

import com.admin.gmall.realtime.bean.TableProcess;
import com.admin.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 自定义类继承BroadcastProcessFunction 处理双流逻辑 ====>广播流：拼接sql语句 根据配置文件动态在hbase库中新建表格
 *                                                 ====>主流：过滤数据 字段  提取出有效字段 为后面在hbase库对应表写入数据做准备 同时分流操作 定义标志位标志数据
 *                                                          发送到kafka 或hbase
 * @author sungh
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 定义属性 状态描述器
    private MapStateDescriptor<String, TableProcess> stateDescriptor;

    // 定义属性 侧输出流标记
    private OutputTag<JSONObject> hBaseTag;

    private Connection connection;

    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        //创建数据库连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor, OutputTag<JSONObject> hBaseTag) {
        this.stateDescriptor = stateDescriptor;
        this.hBaseTag = hBaseTag;
    }

    //处理广播流  数据格式  value:{"db":"","tn":"","data":{"sourceTable":"",...},"before":{},"type":"insert"}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //解析数据
        JSONObject json = JSONObject.parseObject(value);
//        System.out.println("json = " + json);
        TableProcess tableProcess = JSONObject.parseObject(json.getString("data"), TableProcess.class);
//        System.out.println("tableProcess = " + tableProcess);

        // 根据操作类型 和 sink类型 判断是否需要在hbase中创建表格
        // todo type按照那个字段为准
        if ("insert".equals(json.getString("type")) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {

            //创建表格 拼接建表语句
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());

        }

        // 写入状态  广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
//        System.out.println("key1 = " + key);
        broadcastState.put(key, tableProcess);

    }

    //建表语句：create table if not exists db.t(id varchar primary key,tm_name varchar) ...
    private void checkTable(String sinkTale, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {

        //预处理字段 id
        if (null == sinkPk) {
            sinkPk = "id";
        }

        //建表其他字段
        if (null == sinkExtend) {
            sinkExtend = "";
        }

        try {
            //拼接sql语句 create table if not exists db.t(id varchar primary key,tm_name varchar) ...
            StringBuilder createTableSql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTale)
                    .append("(");

            // 处理建表字段
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //判断是否是主键
                if (columns[i].equals(sinkPk)) {
                    createTableSql.append(columns[i]).append(" varchar primary key");
                } else {
                    createTableSql.append(columns[i]).append(" varchar");
                }

                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
            //拼接最后一个括号  sql拼接完成
            createTableSql.append(")").append(sinkExtend);

            System.out.println("createTableSql = " + createTableSql);

            //预编译
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            //执行
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("表" + sinkTale + "sql创建失败");
        } finally {
            preparedStatement.close();
        }


    }

    //value:{"database":"","tableName":"","data":{"id":"","tm_name":"","logo_url":""},"before":{},"type":"insert"}
    //主流  过滤数据  分流到kafka 或者hbase
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //获取广播流
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        String key = value.getString("tableName") + "_" + value.getString("type");
//        System.out.println("key2 = " + key);
        TableProcess tableProcess = broadcastState.get(key);

        if (null != tableProcess) {
            //过滤数据
            filterColumn(tableProcess.getSinkColumns(), value.getJSONObject("data"));

            //分流
            String sinkType = tableProcess.getSinkType();
            String sinkTable = tableProcess.getSinkTable();
            value.put("sinkTable",sinkTable);

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //输出到侧输出流
                ctx.output(hBaseTag,value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //输出到主流
                out.collect(value);
            }

        } else {
            System.out.println(value.getString("tableName") + "数据不存在");
        }

    }

    private void filterColumn(String sinkColumns, JSONObject data) {
        String[] cols = sinkColumns.split(",");
        //转换成list集合
        List<String> list = Arrays.asList(cols);

        //将不需要的字段过滤掉
        data.entrySet().removeIf(stringObjectEntry -> !list.contains(stringObjectEntry.getKey()));


    }


}
