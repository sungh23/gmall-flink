package com.admin.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author sungh
 */
public class KeywordUtil {

    public static List<String> splitKeyWord(String keyword,boolean useSmart){

        //创建用于返回结果的集合
        List<String> resultList = new ArrayList<>();

        // 将字符串转换为流
        StringReader reader = new StringReader(keyword);

        //获取IK分词对象  useSmart字段是否只使用一次
        IKSegmenter ikSegmenter = new IKSegmenter(reader, useSmart);

        Lexeme next = null;
        try {
            next = ikSegmenter.next();
            while (next != null) {
                //取出切好的分词
                resultList.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultList;

    }


    public static void main(String[] args) {
        System.out.println(splitKeyWord("Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",true));
    }

}
