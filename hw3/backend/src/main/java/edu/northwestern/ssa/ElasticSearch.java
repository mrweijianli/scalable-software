package edu.northwestern.ssa;

import org.apache.commons.io.IOUtils;
import org.joda.time.LocalTime;
import org.json.JSONObject;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


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
                System.out.println(IOUtils.toString(abortableInputStream, StandardCharsets.UTF_8));
                abortableInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
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
    }

    // index
    public void createBulkDocuments(List<List<Object>> listOfDocs) throws IOException {
        String indexingMeta = "{ \"index\" : {\"_index\" : \"" + System.getenv("ELASTIC_SEARCH_INDEX") + "\"}}\n";
        StringBuilder jsonBody = new StringBuilder();
        String docTitle;
        String docText;
        String docLink;
        String contentToIndex;
        Document document;
        JSONObject jsonObject = new JSONObject();
        for(List<Object> doc : listOfDocs) {
            document = (Document) doc.get(0);
            docTitle = document.title();
            docText = document.text();
            docLink = (String) doc.get(1);
            jsonObject.put("title", docTitle);
            jsonObject.put("txt", docText);
            jsonObject.put("url", docLink);
            contentToIndex = jsonObject.toString()+"\n";
            jsonBody.append(indexingMeta);
            jsonBody.append(contentToIndex);
        }
        jsonBody.append("\n");
        String body = jsonBody.toString();
        System.out.println(new LocalTime());
        HttpExecuteResponse httpExecuteResponse = restRequest(SdkHttpMethod.POST, hostName, elasticIndex + "/_bulk", Optional.empty(), body);
        httpExecuteResponse.responseBody().ifPresent(abortableInputStream -> {
            try {
                abortableInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        System.out.println(new LocalTime());
//        System.out.println(httpExecuteResponse.httpResponse().statusCode());
//        System.out.println(httpExecuteResponse.httpResponse().statusText());
    }

    public void createBulkDocuments(ArrayList<String> listOfDocs) throws IOException {
        String indexingMeta = "{ \"index\" : {\"_index\" : \"" + System.getenv("ELASTIC_SEARCH_INDEX") + "\"}}\n";
        StringBuilder jsonBody = new StringBuilder();
        for(String doc : listOfDocs) {
            jsonBody.append(doc);
        }
        jsonBody.append("\n");
        String body = jsonBody.toString();
        HttpExecuteResponse httpExecuteResponse = restRequest(SdkHttpMethod.POST, hostName, elasticIndex + "/_bulk", Optional.empty(), body);

        if (httpExecuteResponse.httpResponse().statusCode() != 200) {
            httpExecuteResponse = restRequest(SdkHttpMethod.POST, hostName, elasticIndex + "/_bulk", Optional.empty(), body);
        }
        httpExecuteResponse.responseBody().ifPresent(abortableInputStream -> {
            try {
//                System.out.println(IOUtils.toString(abortableInputStream, StandardCharsets.UTF_8));
//                JSONObject response = new JSONObject(IOUtils.toString(abortableInputStream, StandardCharsets.UTF_8));
//                if ((int)response.getJSONObject("_shards").get("failed") > 0) {
//                    restRequest(SdkHttpMethod.POST, hostName, elasticIndex + "/_bulk", Optional.empty(), body);
//                }
                abortableInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
//        System.out.println(new LocalTime());
//        System.out.println(httpExecuteResponse.httpResponse().statusCode());
//        System.out.println(httpExecuteResponse.httpResponse().statusText());
//        System.out.println(httpExecuteResponse.responseBody());
    }

    public void search(String key, String value) throws IOException{
        Map<String, String> query = new HashMap<>();
        query.put(key, value);
        HttpExecuteResponse httpResponse = restRequest(SdkHttpMethod.GET, hostName, elasticIndex + "/_doc/_search?q="+value, Optional.of(query), Optional.empty());
    }
}
