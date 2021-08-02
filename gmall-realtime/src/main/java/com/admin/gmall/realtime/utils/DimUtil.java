package com.admin.gmall.realtime.utils;

import com.admin.gmall.realtime.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @author sungh
 */
public class DimUtil {

    /**
     * 查询redis中缓存数据  如果没有 则从数据库中查
     * @param connection
     * @param tableName
     * @param id
     * @return
     */
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) {

        // 查询redis 中数据  有直接返回
        String redisKey = "DIM:" + tableName + ":" + id;
        Jedis jedis = RedisUtil.getJedis();
        //查询redis中数据
        String dimInfo = jedis.get(redisKey);

        if (dimInfo != null) {
            JSONObject jsonObject = JSONObject.parseObject(dimInfo);

            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            // 放回连接
            jedis.close();
            return jsonObject;
        }
        // redis中没有的数据  使用工具类查库
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + id + "'";
        List<JSONObject> queryList = JdbcUtil.queryList(connection,
                querySql,
                JSONObject.class,
                false);

        // 写入到redis中  同时设置过期时间为 24小时
        JSONObject dimInfoJson = queryList.get(0);
        jedis.set(redisKey, dimInfoJson.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);

        // 放回连接 返回数据
        jedis.close();
        return dimInfoJson;

    }

    /**
     * 根据redisKey值删除数据
     * @param redisKey
     */
    public static void delDimInfo(String redisKey) {
        Jedis jedis = RedisUtil.getJedis();

        jedis.del(redisKey);

        jedis.close();
    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        "select * from GMALL210225_REALTIME.HBASE_TRADEMARK"

        long start = System.currentTimeMillis();
        JSONObject info = getDimInfo(connection, "HBASE_TRADEMARK", "17");
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        JSONObject info1 = getDimInfo(connection, "HBASE_TRADEMARK", "17");

        long end2 = System.currentTimeMillis();
        System.out.println(end2-end);


    }
}
