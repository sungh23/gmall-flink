package com.admin.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sungh
 */
public class JdbcUtil {

    // 封装查询工具类 返回类型使用泛型 传入参数为 connect sql class toCamel
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean toCamel) {

        //创建返回集合
        ArrayList<T> list = new ArrayList<>();

        PreparedStatement preparedStatement = null;

        try {
            //编译sql
            preparedStatement=connection.prepareStatement(querySql);
            // 执行
            ResultSet resultSet = preparedStatement.executeQuery();

            //查询元数据  -->表中的字段信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            //字段总数
            int columnCount = metaData.getColumnCount();

            //封装数据
            while (resultSet.next()) {

                //创建泛型
                T t = clz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    //获取字段明
                    String columnName = metaData.getColumnName(i);
                    //转换为小驼峰
                    if (toCamel) {
                        columnName= CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    // 获取字段值
                    String value = resultSet.getString(i);

                    //封装到javaBean中
                    BeanUtils.setProperty(t,columnName,value);
                }

                //添加到集合里面
                list.add(t);

            }


        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // 取出结果 封装
        return list;

    }


}
