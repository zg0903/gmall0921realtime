package com.zg.gmall0921.publisher.service;

import java.util.LinkedHashSet;
import java.util.Map;

/**
 * @author zhuguang
 * @Project_name gmall0921realtime
 * @Package_name com.zg.gmall0921.publisher.service
 * @date 2021-01-27-11:37
 */
public interface DauService {
    public String getDate(String name);


    public long getTotal(String date);


    public Map getHourCount(String date);
}
