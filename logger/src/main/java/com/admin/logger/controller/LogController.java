package com.admin.logger.controller;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.security.Principal;

/**
 * @author sungaohua
 */
@RestController
@Slf4j
public class LogController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "applog", method = RequestMethod.GET)
    public String getLogger(@RequestParam("param") String logStr) {

        //将数据写入到日志文件 落盘
        log.info(logStr);

        //将数据发送到kafka
        kafkaTemplate.send("ods_base_log", logStr);

        return "success";

    }

    @RequestMapping(value = "test", method = RequestMethod.GET)
    public String test() {

        System.out.println("success");

        return "success";

    }

}
