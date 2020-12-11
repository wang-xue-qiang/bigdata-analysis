package com.pusidun.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class TimestampController {
    /**
     * 获取系统时间戳
     * @return
     */
    @RequestMapping(value = "/u3d/v2/getTimestamp", produces = {"application/json; charset=UTF-8"})
    public Map<String, Object> getTimestamp() {
        Map<String, Object> map = new HashMap<>();
        map.put("data", new Date().getTime());
        return map;
    }
}
