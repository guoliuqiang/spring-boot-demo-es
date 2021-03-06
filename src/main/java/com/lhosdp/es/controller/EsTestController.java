package com.lhosdp.es.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试对document的增删改查
 */
@RestController
@RequestMapping("/doc")
public class EsTestController {

    @Autowired
    private RestHighLevelClient client;

    @GetMapping("/getDocumment")
    public String getDocument() throws IOException {
        System.out.println("测试");
        //构建请求
        GetRequest request = new GetRequest("book", "1");

        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        //结果
        System.out.println(response.getSource().toString());
        //判断结果是否拿到了
        if (response.isExists()) {
            response.getSourceAsString();//以字符串形式获取
            response.getSourceAsBytes();//以bytes形式获取
            response.getSourceAsMap();//以map形式获取
        }
        return response.getSourceAsString();
    }


    /**
     * 测试可选字段
     * 可以再请求上设置一些过滤条件
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/getFetchSource")
    public String getFetchSource() throws IOException {
        System.out.println("测试");
        //构建请求
        GetRequest request = new GetRequest("book", "1");

        //包含的字段
        String[] includes = new String[]{"name", "description"};
        //排除的字段
        String[] excutedes = Strings.EMPTY_ARRAY;
        //构建
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excutedes);
        //设置可选的字段条件对象
        request.fetchSourceContext(fetchSourceContext);
        //执行
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        //结果
        System.out.println(response.getSource().toString());
        return response.getSourceAsString();
    }


    /**
     * 测试异步查询
     *
     * @return
     * @throws IOException
     */
    @GetMapping("/getFetchSourceSync")
    public String getFetchSourceSync() throws IOException {
        System.out.println("异步查询测试");
        //构建请求
        GetRequest request = new GetRequest("book", "1");

        //包含的字段
        String[] includes = new String[]{"name", "description"};
        //排除的字段
        String[] excutedes = Strings.EMPTY_ARRAY;
        //构建
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excutedes);
        //设置可选的字段条件对象
        request.fetchSourceContext(fetchSourceContext);

        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            //执行成功
            @Override
            public void onResponse(GetResponse o) {
                System.out.println("数据：：" + o.getSource().toString());
            }

            //执行失败
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        //异步执行
        client.getAsync(request, RequestOptions.DEFAULT, listener);


//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        return "success";
    }


    /**
     * 插入数据
     */
    @GetMapping("/insertMessage")
    public void insertMessage() throws IOException {
        //构建请求
        IndexRequest request = new IndexRequest("book_test");
        request.id("3");
        //构建文档数据，四种类型
        //字符串类型
//        String jsonstring = "{\n" +
//                "\"name\":\"bootstarp开发3\",\n" +
//                "\"description\":\"我是描述\",\n" +
//                "\"studymodel\":\"201002\"\n" +
//                "}";
//        request.source(jsonstring, XContentType.JSON);
        //json类型,构建一个json
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("name","bootstarp开发3");
//            builder.field("description","我是描述json");
//            builder.field("studymodel","201002");
//        }
//        builder.endObject();
//        request.source(builder,XContentType.JSON);

//        map类型
        HashMap<String, Object> map = new HashMap<>();
        map.put("name","bootstarp开发4");
        map.put("description","我是描述4");
        map.put("studymodel","201002");
        request.source(map);

        //方法4
        //request.source("name","bootstarp开发3","description","我是描述json","studymodel","201002");

//        ================可选参数====================
        //设置超时时间
//        request.timeout("1s");
//        request.timeout(TimeValue.timeValueSeconds(1));

        //手动维护版本号
//        request.version(3);
//        request.versionType(VersionType.EXTERNAL);

        //执行
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("版本号" + response.getVersion());
        System.out.println("结果" + response.getResult());

        this.checkShare(response.getShardInfo());

    }


    /**
     * 异步插入数据，
     * 获取响应结果作出不同操作
     */
    @GetMapping("/asyncInsertMessage")
    public void AsyncInsertMessage() throws IOException {
        //构建请求
        IndexRequest request = new IndexRequest("book_test");
        request.id("5");

        request.source("name", "bootstarp开发5", "description", "我是描述json", "studymodel", "201002");

//        ================可选参数====================
        //设置超时时间
        request.timeout("1s");
        request.timeout(TimeValue.timeValueSeconds(1));

        //手动维护版本号--同一个版本号不行执行两次，ES的防并发操作
        request.version(4);
        request.versionType(VersionType.EXTERNAL);

        //执行--同步
//        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //执行--异步
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            //成功时执行
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("版本号" + indexResponse.getVersion());
                System.out.println("结果" + indexResponse.getResult());
                System.out.println("======");
                checkResult(indexResponse.getResult());
                System.out.println(indexResponse.getId());
            }

            //失败时执行
            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };


        client.indexAsync(request, RequestOptions.DEFAULT, listener);
        //线程睡眠
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    /**
     * 测试局部修改
     */
    @GetMapping("/updateMesage")
    public void updateMessage() throws IOException {
        //创建请求
        UpdateRequest request = new UpdateRequest("test_update","2");
        Map<String, Object> map = new HashMap<>();
        map.put("name", "我是郭刘强");
        //设置请求数据doc
        request.doc(map);
        //可选参数
        request.timeout("1s");
        request.retryOnConflict(3);//重试次数，3次之后失败就放弃

        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.getIndex());
        this.checkResult(update.getResult());
        this.checkShare(update.getShardInfo());

    }

    /**
     * 测试删除
     */
    @GetMapping("/testDelete")
    public void testDelete() throws IOException {
        //创建请求
        DeleteRequest deleteRequest = new DeleteRequest("test_update","2");
        //设置可选参数

        //执行
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        checkShare(delete.getShardInfo());
        checkResult(delete.getResult());

    }

    /**
     * 测试批量操作
     */
    @GetMapping("/bulk")
    public void testbulk() throws IOException {
        //创建请求
        BulkRequest request = new BulkRequest();
        //添加
        request.add(new IndexRequest("tvs").id("1")
                .source(XContentType.JSON, "price", "1000", "color","红色","brand","长虹","sold_date","2019-10-28"));
        request.add(new IndexRequest("tvs").id("2")
                .source(XContentType.JSON, "price", "2000", "color","红色","brand","长虹","sold_date","2019-11-05"));
        request.add(new IndexRequest("tvs").id("3")
                .source(XContentType.JSON, "price", "3000", "color","绿色","brand","小米","sold_date","2019-05-18"));
        request.add(new IndexRequest("tvs").id("4")
                .source(XContentType.JSON, "price", "1500", "color","蓝色","brand","TCL","sold_date","2019-07-02"));
        request.add(new IndexRequest("tvs").id("5")
                .source(XContentType.JSON, "price", "1200", "color","绿色","brand","TCL","sold_date","2019-08-19"));
        request.add(new IndexRequest("tvs").id("6")
                .source(XContentType.JSON, "price", "2000", "color","红色","brand","长虹","sold_date","2019-11-05"));
        request.add(new IndexRequest("tvs").id("7")
                .source(XContentType.JSON, "price", "8000", "color","红色","brand","三星","sold_date","2020-01-01"));
        request.add(new IndexRequest("tvs").id("8")
                .source(XContentType.JSON, "price", "2500", "color","蓝色","brand","小米","sold_date","2020-02-12"));

//        request.add(new IndexRequest("post").id("2").source(XContentType.JSON, "filed", "2"));
//
//        //修改
//        request.add(new UpdateRequest("post","2").doc(XContentType.JSON, "filed","3"));
//        //删除
//        request.add(new DeleteRequest("post","2"));

        //执行
        BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
        this.checkBulkResult(bulk);

    }

    /**
     * 批量请求返回结果处理
     */
    public void checkBulkResult(BulkResponse bulkItemResponses){
        for (BulkItemResponse bulkItemRespons : bulkItemResponses) {
            DocWriteResponse response = bulkItemRespons.getResponse();
            switch (bulkItemRespons.getOpType()){
                case INDEX:
                    IndexResponse indexResponse = (IndexResponse)response;
                    this.checkResult(indexResponse.getResult());
                    break;
                case CREATE:
                    IndexResponse indexRespons1 = (IndexResponse) response;
                    this.checkResult(indexRespons1.getResult());
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) response;
                    this.checkResult(deleteResponse.getResult());
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) response;
                    this.checkResult(updateResponse.getResult());
                    break;
            }
        }
    }

    /**
     * 返回请求结果匹配数据
     * @param result
     * @return
     */
    private void checkResult(DocWriteResponse.Result result){
        if(result == DocWriteResponse.Result.CREATED){
            System.out.println("CREATED :" + result);
        }else if(result == DocWriteResponse.Result.UPDATED){
            System.out.println("UPDATED :" + result);
        }else if(result == DocWriteResponse.Result.DELETED){
            System.out.println("DELETED :" + result);
        }else if(result == DocWriteResponse.Result.NOOP){
            System.out.println("NOOP :" + result);
            //没有任何操作，如果数据与原来的数据相同
        }else {
            System.out.println("I don't know");
        }
    }

    //检查分片信息
    private void checkShare(ReplicationResponse.ShardInfo shardInfo){
        //判断总分片数是否等于执行成功的分片数量
        if(shardInfo.getTotal() != shardInfo.getSuccessful()){
            System.out.println("输出一些警告信息");
        }
        //如果执行失败分片数量大于0
        if(shardInfo.getFailed() > 0){
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();//错误原因
                System.out.println(reason);
            }
        }
    }
}
