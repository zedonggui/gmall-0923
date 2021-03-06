package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {
    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date")String date){
        //1.创建list集合用来保存结果数据
        ArrayList<Map> result = new ArrayList<>();
        //2.创建map集合用来存放新增日活返回的结果
        HashMap<String, Object> dauMap = new HashMap<>();
        //3.创建map集合用来存放新增设备返回的结果
        HashMap<String, Object> devMap = new HashMap<>();
        //创建map集合用来存放新增交易额返回的结果
        HashMap<String, Object> gmvMap = new HashMap<>();

        //日活总数
        Integer total = publisherService.getDauTotal(date);
        //4.往map里添加数据
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value",total);

        devMap.put("id", "new_mid");
        devMap.put("name", "新增设备");
        devMap.put("value",233);

        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", publisherService.getGmvTotal(date));

        //5.将map添加到list集合
        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);


        return JSON.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam("id")String id,
                                  @RequestParam("date")String date){

        //2.创建Map集合用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();
        Map todayMap = null;
        Map yesterdayMap = null;
        //3.获取昨天日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        if (id.equals("dau")){
            //1.拿取当天service分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //4.拿取昨天天service分时数据
           yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if (id.equals("order_amount")){
            todayMap = publisherService.getGmvTotalHourMap(date);
            //4.拿取昨天天service分时数据
            yesterdayMap = publisherService.getGmvTotalHourMap(yesterday);
        }
        //5.将数据保存至map集合
        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return JSON.toJSONString(result);
    }

}
