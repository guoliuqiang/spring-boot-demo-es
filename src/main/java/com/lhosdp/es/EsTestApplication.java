package com.lhosdp.es;


import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class EsTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsTestApplication.class, args);

	}

	@Autowired
	private RestHighLevelClient client;

	@EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() throws Exception {
		System.out.println("测试");
		//构建请求
		GetRequest request = new GetRequest("book","1");

		//执行
		GetResponse response = client.get(request, RequestOptions.DEFAULT);

		//结果
		System.out.println(response.getId());
		System.out.println(response.getSource().toString());

	}
}
