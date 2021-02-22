package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private DauMapper dauMapper;

    @Override
    public Integer getDauTotal(String date) {

        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHourMap(String date) {
        //1.获取数据
        List<Map> dauHourList = dauMapper.selectDauTotalHourMap(date);

        //创建map集合用于存放结果数据
        HashMap<String, Long> result = new HashMap<>();

        //2.遍历List集合
        for (Map map : dauHourList) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }
        return result;
    }
}
