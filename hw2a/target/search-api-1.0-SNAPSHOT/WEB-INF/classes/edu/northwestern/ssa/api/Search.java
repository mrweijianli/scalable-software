package edu.northwestern.ssa.api;

import edu.northwestern.ssa.AwsSignedRestRequest;
import edu.northwestern.ssa.ElasticSearch;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.utils.IoUtils;
import sun.misc.IOUtils;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("search")
public class Search {

    /** when testing, this is reachable at http://localhost:8080/api/search?query=hello */
//    @GET
//    public Response getMsg(@QueryParam("query") String q) throws IOException {
//        JSONArray results = new JSONArray();
//        results.put("hello world!");
//        results.put(q);
//        return Response.status(200).type("application/json").entity(results.toString(4))
//                // below header is for CORS
//                .header("Access-Control-Allow-Origin", "*").build();
//    }

    ElasticSearch elasticSearch = new ElasticSearch("es");

    /** search the elastic search and return results **/
    @GET
    public Response getMsg(@QueryParam("query") String query,
                           @DefaultValue("null") @QueryParam("language") String lang,
                           @DefaultValue("null") @QueryParam("date") String date,
                           @DefaultValue("null") @QueryParam("count") String count,
                           @DefaultValue("null") @QueryParam("offset") String offset) throws IOException {
        JSONArray results = new JSONArray();
        JSONObject jsonObjectResults = new JSONObject();
        HttpExecuteResponse httpExecuteResponse = null;
        if (query == null) return Response.status(400).header("Access-Control-Allow-Origin", "*").build();
//        if (offset.equals("null"))
//        httpExecuteResponse = elasticSearch.searchRequest(query, lang, date, count, offset);
//        else {
//            int from = 0;
//            for (int i=0; i<Integer.getInteger(count); i++) {
//                httpExecuteResponse = elasticSearch.searchRequest(query, lang, date, "1", Integer.toString(from));
//                from += Integer.getInteger(offset);
//            }
//        }
//        if (Integer.getInteger(count) == 0) httpExecuteResponse = elasticSearch.searchRequest(query, lang, date, "0", "null");

        httpExecuteResponse = elasticSearch.searchRequest(query, lang, date, count, offset);
        httpExecuteResponse.responseBody().ifPresent(abortableInputStream -> {
            try {
                JSONObject jsonObject = new JSONObject(IoUtils.toUtf8String(abortableInputStream));
                jsonObjectResults.put("total_results", jsonObject.getJSONObject("hits").getJSONObject("total").get("value"));
                int returned_results = jsonObject.getJSONObject("hits").getJSONArray("hits").length();
                jsonObjectResults.put("returned_results", returned_results);
                JSONArray jsonArray = new JSONArray();
                for (int i=0; i<returned_results; i++) {
                    jsonArray.put(jsonObject.getJSONObject("hits").getJSONArray("hits").getJSONObject(i).getJSONObject("_source"));
                }
                jsonObjectResults.put("articles", jsonArray);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return Response.status(200).type("application/json").entity(jsonObjectResults.toString(4))
                // below header is for CORS
                .header("Access-Control-Allow-Origin", "*").build();
    }
}
