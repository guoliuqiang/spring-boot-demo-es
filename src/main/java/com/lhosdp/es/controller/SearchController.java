package com.lhosdp.es.controller;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Map;

/**
 * 搜索查询示例
 */
@RestController
@RequestMapping("/search")
public class SearchController {


    @Autowired
    RestHighLevelClient client;

    /**
     * 查询所有数据，可以过滤字段
     * @throws IOException
     */
    @GetMapping("/searchAll")
    public void searchAll() throws IOException {

        //构建搜索请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //对字段进行过滤，只要name字段，不要别的
        searchSourceBuilder.fetchSource(new String[]{"name"}, Strings.EMPTY_ARRAY);
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取结果
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("-----------");
            System.out.println(hit.getIndex());
            Map<String, Object> map = hit.getSourceAsMap();

            System.out.println("name" + map.get("name"));
            System.out.println("description" + map.get("description"));
            System.out.println("price" + map.get("price"));
        }

    }

    /**
     * 分页查询
     * 按照关键词进行搜索
     * 按照term进行搜索
     * 按multiMatch进行搜索
     * @throws IOException
     */
    @GetMapping("/searchAllpage")
    public void searchAllpage() throws IOException {

        //构建搜索请求
        SearchRequest searchRequest = new SearchRequest("book");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询所有
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //对字段进行过滤，只要name字段，不要别的
        //searchSourceBuilder.fetchSource(new String[]{"name"}, Strings.EMPTY_ARRAY);
        /*
        分页
         */
//        int page = 1;
//        int size = 2;
//        int from = (page - 1) * size;
//        searchSourceBuilder.from(from);
//        searchSourceBuilder.size(size);
        /*
        根据ids进行搜索
         */
        //searchSourceBuilder.query(QueryBuilders.idsQuery().addIds("1","12","9"));
        /*
        按关键词进行搜索
         */
//        searchSourceBuilder.query(QueryBuilders.matchQuery("description","我"));
        /*
        按term进行搜索
         */
//        searchSourceBuilder.query(QueryBuilders.termQuery("description", "是"));
        /*
        按multiMatch进行搜索,按照多个字段搜索关键词，java程序员是关键词，后边是字段。
         */
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("java程序员","name","description"));

        searchRequest.source(searchSourceBuilder);
        //执行搜索
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取结果
        SearchHit[] hits = search.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("-----------");
            System.out.println(hit.getIndex());

            Map<String, Object> map = hit.getSourceAsMap();
            System.out.println("id" + hit.getId());
            System.out.println("name" + map.get("name"));
            System.out.println("description" + map.get("description"));
            System.out.println("price" + map.get("price"));
        }

    }
}
