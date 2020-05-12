package com.lhosdp.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticTestClient {

    public static void main(String[] args) throws IOException {
        //创建host
        HttpHost httpHost = new HttpHost("114.67.126.146", 9200, "http");
        //创建客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(httpHost));

        //构建请求
        GetRequest request = new GetRequest("book","1");

        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        //结果
        System.out.println(response.getId());
    }
}
