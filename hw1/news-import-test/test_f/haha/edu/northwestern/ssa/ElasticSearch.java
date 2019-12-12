package edu.northwestern.ssa;

import org.json.JSONObject;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElasticSearch extends AwsSignedRestRequest {
    private String hostName = System.getenv("ELASTIC_SEARCH_HOST");
    private String elasticIndex = System.getenv("ELASTIC_SEARCH_INDEX");

    public ElasticSearch(String serviceName) {
        super(serviceName);
    }

    public void createElasticIndex() throws IOException {
        HttpExecuteResponse httpResponse = restRequest(SdkHttpMethod.PUT, hostName, elasticIndex, Optional.empty(), Optional.empty());
        httpResponse.responseBody().ifPresent(abortableInputStream -> {
            try {
                abortableInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println(httpResponse.httpResponse().statusCode());
    }

    public void createDocument(Document document, String url) throws IOException {
        JSONObject body = new JSONObject();
        body.put("url", url);
        body.put("title", document.title());
        body.put("txt", document.text());
        HttpExecuteResponse httpResponse = restRequest(SdkHttpMethod.POST, hostName, elasticIndex + "/_doc/", Optional.empty(), Optional.of(body));
        httpResponse.responseBody().ifPresent(abortableInputStream -> {
            try {
                abortableInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println("created: "+document.title());
    }

    public void search(String key, String value) throws IOException{
        Map<String, String> query = new HashMap<>();
        query.put(key, value);
        HttpExecuteResponse httpResponse = restRequest(SdkHttpMethod.GET, hostName, elasticIndex + "/_doc/_search?q="+value, Optional.of(query), Optional.empty());
    }
}
