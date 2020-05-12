package com.lhsodp.es;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DemoTest {


    @Autowired
    private RestHighLevelClient client;

    @Test
    public void testGet() throws IOException {
        //构建请求
        GetRequest request = new GetRequest("book","1");

        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        //结果
        System.out.println(response.getId());
    }
}

