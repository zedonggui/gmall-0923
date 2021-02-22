package com.atguigu.gmallpublisher.service;

import java.util.List;
import java.util.Map;

public interface PublisherService {
    //日活总数
    public Integer getDauTotal(String date);

    //分时数据
    public Map getDauTotalHourMap(String date);

    //GMV总数
    public Double getGmvTotal(String date);

    //GMV分时数据
    public Map getGmvTotalHourMap(String date);


}
