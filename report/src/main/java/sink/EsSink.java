package sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @BelongsProject: flink
 * @BelongsPackage: sink
 * @Author: luk@jiguang.cn
 * @CreateTime: 2019-10-22 10:16
 */

public class EsSink  {

    public static ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> getEsSink() {
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("cts04", 9200,"http"));

        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple4<String, String, String, Long>>() {
                    public IndexRequest createIndexRequest(Tuple4<String, String, String, Long> element) {
                        HashMap<String, Object> json = new HashMap<>();
                        json.put("time", element.f0);
                        json.put("type", element.f1);
                        json.put("area", element.f2);
                        json.put("count", element.f3);

                        return Requests.indexRequest()
                                .index("audit_index")
                                .type("audit_type")
                                .source(json);
                    }


                    @Override
                    public void process(Tuple4<String, String, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }

        );

        return esSinkBuilder;
    }

}