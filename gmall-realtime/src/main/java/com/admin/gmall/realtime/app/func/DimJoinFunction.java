package com.admin.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author sungh
 */
public interface DimJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}
