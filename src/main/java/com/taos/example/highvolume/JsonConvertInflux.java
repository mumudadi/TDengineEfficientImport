package com.taos.example.highvolume;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author mumud
 */
public class JsonConvertInflux {
    public static List<String> convert(List<String> jsonList, String topic,String tagKey) {
        return jsonList.stream().map(json -> convert(JSONUtil.parseObj(json),topic,tagKey)).collect(Collectors.toList());
    }
    public static List<String> convert(String jsonStr, String topic,String tagKey) {
        if(JSONUtil.isTypeJSONArray(jsonStr)) {
            JSONArray jsonArray = JSONUtil.parseArray(jsonStr);
            return jsonArray.stream().map(a -> convert(JSONUtil.parseObj(a),topic,tagKey)).collect(Collectors.toList());
        }
        if(JSONUtil.isTypeJSONObject(jsonStr)) {
            JSONObject jsonObj = JSONUtil.parseObj(jsonStr);
            return CollUtil.newArrayList(convert(jsonObj,topic,tagKey));
        }
        return null;
    }
    public static String convert(JSONObject jsonObj,String topic,String tagKey) {

        //构造InfluxDB 格式数据
        String tagVlaue = jsonObj.getStr(tagKey);
        if(StrUtil.isEmpty(tagVlaue)) {
            tagVlaue = "null";
        }
        StrBuilder influxData = StrBuilder.create(topic.toLowerCase());
        influxData.append(",tname=").append(topic.toLowerCase()).append("_").append(tagVlaue.toLowerCase())
                .append(",").append(underline(tagKey)).append("=").append(tagVlaue).append(" ");
        for(String key : jsonObj.keySet()) {
            if("_ts".equals(key) || tagKey.equals(key)) {
                continue;
            }
            influxData.append(underline(key)).append("=");
            Object value = jsonObj.get(key);
            if(value instanceof BigDecimal || value instanceof Integer) {
                influxData.append(value).append("f32");
            } else if(value instanceof Long) {
                influxData.append(value).append("i64");
            } else {
                if(value instanceof Date) {
                    value = DateUtil.formatDateTime((Date) value);
                }
                influxData.append("L\"").append(value).append("\"");
            }
            influxData.append(",");
        }
        influxData.delTo(influxData.length()-1);
        influxData.append(" ");
        if(ObjUtil.isNotEmpty(jsonObj.getLong("_ts"))) {
            influxData.append(jsonObj.getLong("_ts"));
        } else {
            influxData.append(System.currentTimeMillis()).append(RandomUtil.randomNumbers(6));
        }
        return influxData.toString();
    }

    /**
     * 将驼峰转为下划线
     */
    public static String underline(String str) {
        Pattern compile = Pattern.compile("[A-Z]");
        Matcher matcher = compile.matcher(str);
        StringBuffer sb = new StringBuffer();
        while(matcher.find()) {
            matcher.appendReplacement(sb,  "_" + matcher.group(0).toLowerCase());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
