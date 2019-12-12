package edu.northwestern.ssa;

import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpMethod;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ElasticSearch extends AwsSignedRestRequest{

    public ElasticSearch (String serviceName) {
        super(serviceName);
    }

    public HttpExecuteResponse searchRequest (String query, String lang, String date, String count, String offset) throws IOException {
        Map<String, String> queryMap = new HashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("txt:(").append(query).append(")");
        if (!lang.equals("null")) stringBuilder.append(" lang:").append(lang);
        if (!date.equals("null")) stringBuilder.append(" date:").append(date);
        queryMap.put("q", stringBuilder.toString());
        if (!count.equals("null")) queryMap.put("size", count);
        queryMap.put("default_operator", "AND");
        if (!offset.equals("null")) queryMap.put("from", offset);
        queryMap.put("track_total_hits", "true");
        return restRequest(SdkHttpMethod.GET, Config.getParam("ELASTIC_SEARCH_HOST"), Config.getParam("ELASTIC_SEARCH_INDEX")+"/_search", Optional.of(queryMap));
    }
}
