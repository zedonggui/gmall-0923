package com.atguigu.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //日活总数
    public Integer selectDauTotal(String date);

    //分数数据
    public List<Map> selectDauTotalHourMap(String date);
}
