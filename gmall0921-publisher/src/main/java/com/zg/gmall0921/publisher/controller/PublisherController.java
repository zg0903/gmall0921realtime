package com.zg.gmall0921.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.zg.gmall0921.publisher.service.DauService;
import com.zg.gmall0921.publisher.service.impl.DauServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhuguang
 * @Project_name gmall0921realtime
 * @Package_name com.zg.gmall0921.publisher.controller
 * @date 2021-01-27-10:58
 */

@RestController
public class PublisherController {

    @Autowired
    DauService dauService;

    @RequestMapping("/hello")
    public String getHeloWord(@RequestParam("name") String name) {

        String data = dauService.getDate(name);

        return "hello " + data;
    }


    @RequestMapping("/realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date) {
        date = date.replace("-", "");
        long total = dauService.getTotal(date);
        String json = "[{\"id\":\"dau\",\"name\":\"新增日活\",\"value\":" + total + "},\n" +
                "{\"id\":\"new_mid\",\"name\":\"新增设备\",\"value\":233} ]\n";
        return json;
    }


    @RequestMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            Map hourCountTdMap = dauService.getHourCount(date);
            String yd = getYd(date);
            Map hourCountYdMap = dauService.getHourCount(yd);
            HashMap<String, Map<String, Long>> rsMap = new HashMap<>();
            rsMap.put("today", hourCountTdMap);
            rsMap.put("yesterday", hourCountYdMap);
            return JSON.toJSONString(rsMap);
        } else {
            return "No this id";
        }
    }

    private String getYd(String td) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(date, -1);
            return simpleDateFormat.format(ydDate);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期转换失败");
        }

    }


}
