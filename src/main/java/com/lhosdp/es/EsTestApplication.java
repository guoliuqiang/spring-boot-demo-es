package com.lhosdp.es;


import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class EsTestApplication implements WebMvcConfigurer {

	public static void main(String[] args) {
		SpringApplication.run(EsTestApplication.class, args);

	}
	@Autowired
	private RestHighLevelClient client;


	@EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() throws Exception {
		System.out.println("测试");


	}
}
