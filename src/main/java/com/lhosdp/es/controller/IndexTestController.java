package com.lhosdp.es.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * java api
 * 操作索引
 */
@RestController
@RequestMapping("/index")
public class IndexTestController {

    @Autowired
    private RestHighLevelClient client;


    /**
     * 创建索引
     */
    @GetMapping("/testCreateIndex")
    public void testCreateIndex() throws IOException {
        /*
        PUT my_index
        {
          "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
          },
          "mappings": {
            "properties": {
              "field1":{
                "type":"text"
              },
              "field2":{
                "type":"text"
              }
            }
          },
          "aliases": {
            "default_index": {}
          }
        }
         */
        //创建请求
        CreateIndexRequest request = new CreateIndexRequest("my_index");
        //请求中设置setting
        request.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "1").build());
        //请求中设置mappings
        //////创建方式1
//        request.mapping("{\n" +
//                "            \"properties\": {\n" +
//                "              \"field1\":{\n" +
//                "                \"type\":\"text\"\n" +
//                "              },\n" +
//                "              \"field2\":{\n" +
//                "                \"type\":\"text\"\n" +
//                "              }\n" +
//                "            }\n" +
//                "          }", XContentType.JSON);
        //////创建方式2     map方式
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> field1 = new HashMap<>();
        field1.put("type", "text");
        //还可以设置别的参数
        //field1.put("analyzer","standard");//分词器
        Map<String, Object> field2 = new HashMap<>();
        field2.put("type", "text");
        properties.put("field1", field1);
        properties.put("field2", field2);
        mappings.put("properties", properties);
        request.mapping(mappings);
        //////创建方式3  与EsTestController中的类似
        //XContentBuilder builder = XContentFactory.jsonBuilder();

        //请求中设置aliases
        request.alias(new Alias("prod_index"));
        //设置可选参数
        //设置超时时间
        request.setTimeout(TimeValue.timeValueSeconds(5));
        //设置主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueSeconds(5));
        //设置创建索引API返回相应之前等待活动分片的数量
        request.waitForActiveShards(ActiveShardCount.from(1));
        //执行
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        //获取参数
        System.out.println("输出");
        System.out.println(createIndexResponse.isAcknowledged());
        System.out.println(createIndexResponse.isShardsAcknowledged());
        System.out.println(createIndexResponse.index());
    }

    /**
     * 创建索引 异步操作
     */
    @GetMapping("/testCreateIndexAsync")
    public void testCreateIndexAsync() throws IOException {

        //创建请求
        CreateIndexRequest request = new CreateIndexRequest("my_index");
        //请求中设置setting
        request.settings(Settings.builder().put("number_of_shards", "1").put("number_of_replicas", "1").build());
        //请求中设置mappings

        //////创建方式2     map方式
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> field1 = new HashMap<>();
        field1.put("type", "text");
        //还可以设置别的参数
        //field1.put("analyzer","standard");//分词器
        Map<String, Object> field2 = new HashMap<>();
        field2.put("type", "text");
        properties.put("field1", field1);
        properties.put("field2", field2);
        mappings.put("properties", properties);
        request.mapping(mappings);
        //////创建方式3  与EsTestController中的类似
        //XContentBuilder builder = XContentFactory.jsonBuilder();
        //请求中设置aliases
        request.alias(new Alias("prod_index"));
        //设置可选参数
        //设置超时时间
        request.setTimeout(TimeValue.timeValueSeconds(5));
        //设置主节点超时时间
        request.setMasterTimeout(TimeValue.timeValueSeconds(5));
        //设置创建索引API返回相应之前等待活动分片的数量
        request.waitForActiveShards(ActiveShardCount.from(1));
        //创建监听器
        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                System.out.println("输出");
                System.out.println(createIndexResponse.isAcknowledged());
                System.out.println(createIndexResponse.isShardsAcknowledged());
                System.out.println(createIndexResponse.index());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        //异步执行
        client.indices().createAsync(request, RequestOptions.DEFAULT, listener);
    }

    /**
     * 删除索引
     */
    @GetMapping("/deleteindex")
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("my_index");
        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    /**
     * 异步删除索引
     */
    @GetMapping("/deleteindexAsync")
    public void testDeleteIndexAsync() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("my_index");
        ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                System.out.println(acknowledgedResponse.isAcknowledged());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
//        AcknowledgedResponse delete = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
//        System.out.println(delete.isAcknowledged());
        client.indices().deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, listener);
    }

    /**
     * 查询索引
     * 查询索引存不存在
     */
    @GetMapping("/testExistIndex")
    public void getIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("my_index");
        //参数
        request.local(false);//从主节点返回本地索引信息状态
        request.humanReadable(false);//以适合人类的格式返回
        request.includeDefaults(true);//是否返回每个索引的所有默认配置

        //执行
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("是否存在" + exists);
    }

    /**
     * 关闭索引
     * 当一个索引中的数据可以当作历史数据保存起来的时候，可以将该索引关闭，不会被查询出来，也不会被进行修改删除操作
     */
    @GetMapping("/closeIndex")
    public void closeIndex() throws IOException {
        CloseIndexRequest request = new CloseIndexRequest("my_index");
        AcknowledgedResponse close = client.indices().close(request, RequestOptions.DEFAULT);
        boolean acknowledged = close.isAcknowledged();
        System.out.println("acknowledged:" + acknowledged);
    }

    /**
     * 开启索引
     */
    @GetMapping("/openIndex")
    public void openIndex() throws IOException {
        OpenIndexRequest request = new OpenIndexRequest("my_index");
        OpenIndexResponse open = client.indices().open(request, RequestOptions.DEFAULT);
        System.out.println(open.isAcknowledged());
        System.out.println(open.isShardsAcknowledged());
    }

}
