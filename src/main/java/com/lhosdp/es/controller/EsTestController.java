package com.lhosdp.es.controller;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;

@RestController
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
                System.out.println(checkResult(indexResponse.getResult()));
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
     * 返回请求结果匹配数据
     * @param result
     * @return
     */
    private String checkResult(DocWriteResponse.Result result){
        if(result == DocWriteResponse.Result.CREATED){
            return "CREATED :" + result;
        }else if(result == DocWriteResponse.Result.UPDATED){
            return "UPDATED :" + result;
        }else if(result == DocWriteResponse.Result.DELETED){
            return "DELETED :" + result;
        }else if(result == DocWriteResponse.Result.NOOP){
            //没有任何操作，如果数据与原来的数据相同
            return "NOOP :" + result;
        }else {
            return "I don't know";
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
