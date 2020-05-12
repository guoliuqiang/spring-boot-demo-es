package com.lhosdp.es.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootConfiguration
public class DemoConfig {

    @Value("${eshostconfig.es.hostlist}")
    private String hostlist;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient(){
        //传入的host可能是多个
        String[] split = hostlist.split(",");
        HttpHost[] httpHost = new HttpHost[split.length];
        //创建host数组
        for (int i = 0; i < split.length; i++) {
            String host = split[i];
            httpHost[i] = new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]), "http");
        }

        return new RestHighLevelClient(RestClient.builder(httpHost));
    }

}
