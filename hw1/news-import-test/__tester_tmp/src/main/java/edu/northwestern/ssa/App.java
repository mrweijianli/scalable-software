package edu.northwestern.ssa;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

import org.jsoup.nodes.Document;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class App {
//    private  String dataPath = "/Users/weijianli/ssa/ssa-skeleton/data/data.warc.gz";
    private static String serviceName = "es";

    private static String createJsonDoc(Document doc, String url) {
        String indexingMeta = "{ \"index\" : {\"_index\" : \"" + System.getenv("ELASTIC_SEARCH_INDEX") + "\"}}\n";
        StringBuilder jsonBody = new StringBuilder();
        String contentToIndex;
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("title", doc.title());
        jsonObject.put("txt", doc.text());
        jsonObject.put("url", url);
        contentToIndex = jsonObject.toString()+"\n";
        jsonBody.append(indexingMeta);
        jsonBody.append(contentToIndex);
        return jsonBody.toString();
    }

    public static void main(String[] args) throws IOException {
        String dataPath = "data.warc.gz";
        System.getenv("AWS_SECRET_ACCESS_KEY");
        System.getenv("AWS_ACCESS_KEY_ID");
        System.getenv("ELASTIC_SEARCH_HOST");
        System.getenv("ELASTIC_SEARCH_INDEX");
        System.getenv("COMMON_CRAWL_FILENAME");


        dataDownload();

        ElasticSearch elasticSearch = new ElasticSearch(serviceName);
        elasticSearch.createElasticIndex();

        File f = new File(dataPath);
        ArchiveReader archiveReader = WARCReaderFactory.get(f);
        Iterator<ArchiveRecord> it = archiveReader.iterator();
        ArchiveRecord record = it.next();
//        ExecutorService executorService = Executors.newFixedThreadPool(4);
        int i=0;
        int sendCount = 0;
        List<List<Object>> listOfDocs = new ArrayList<>();
        ArrayList<String> listOfDocStrings = new ArrayList<>();
//        Set<Callable<List<Object>>> callables = new HashSet<Callable<List<Object>>>();
        while (it.hasNext()) {
            record = it.next();

//            callables.add(new MyCallable(record));
//            sendCount++;


            /*==========================================================================*/
            String docUrl = record.getHeader().getUrl();
//            byte[] recordChars = new byte[record.available()+1];
//                        record.read(recordChars);
            InputStream inputStream = new BufferedInputStream(record);

            char[] recordChars = new char[inputStream.available()+1];
            int n = 0;
            int x = 0;
            while (x != -1) {
                x = inputStream.read();
                if (x == 0) {
                    continue;
                }
                recordChars[n] = (char) x;
                n++;
            }



            String content = new String(recordChars);

            if (recordChars[0] != 'G') {
                String[] contentTmp = (content.split("\\r\\n\\r\\n", 2));
                content = contentTmp[1];
                Document htmlDoc = Jsoup.parse(content);
//                List<Object> doc = new ArrayList<>();
//                doc.add(htmlDoc);
//                doc.add(docUrl);
//                listOfDocs.add(doc);
                sendCount++;
                String jsonDoc = createJsonDoc(htmlDoc, docUrl);
                listOfDocStrings.add(jsonDoc);


//                callables.add(new MyCallable(content, docUrl));


//                Runnable runnableTask = () -> {
//                    try {
//                        // create the index for this document on ElasticSearch
//                        ElasticSearch elasticSearch1 = new ElasticSearch(serviceName);
//                        elasticSearch1.createDocument(htmlDoc, docUrl);
////                        System.out.println(i++);
//                    } catch (IOException e) {
//
//                    }
//                };
//                MyRunnable runnableTask = new MyRunnable(serviceName, htmlDoc, docUrl);
//                executorService.execute(runnableTask);
            }

            /*==========================================================================*/

            // do indexing by bulk
            if (sendCount == 200 || (!it.hasNext())) {
                elasticSearch.createBulkDocuments(listOfDocStrings);
//                elasticSearch.createBulkDocuments(listOfDocs);
                sendCount = 0;
//                listOfDocs.clear();
                listOfDocStrings.clear();
            }
            inputStream.close();

        }
        elasticSearch.close();
    }

    private static void dataDownload() {
        String dataPath = "./data.warc.gz";
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

        String bucket = "commoncrawl";
        String key = System.getenv("COMMON_CRAWL_FILENAME");
        System.out.println(key);
        // if no filename specified, get the latest one
        if (key == null || key.isEmpty()) {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").build();
            ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
            S3Object latestObject = listObjectsV2Response.contents().get(0);
            while (listObjectsV2Response.isTruncated()) {
//                if (!listObjectsV2Response.contents().get(999).key().endsWith(".warc.gz")) {
//                    listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").continuationToken(listObjectsV2Response.nextContinuationToken()).build();
//                    listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
//                    continue;
//                }
//                int i = 1;
//                while (!latestObject.key().endsWith(".warc.gz") && i < 1000) {
//                    latestObject = listObjectsV2Response.contents().get(i);
//                    System.out.println(latestObject.key());
//                    i++;
//                }
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    if (s3Object.key().endsWith(".warc.gz") && latestObject.lastModified().compareTo(s3Object.lastModified()) < 1) {
                        latestObject = s3Object;
                    }
                }
                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").continuationToken(listObjectsV2Response.nextContinuationToken()).build();
                listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
            }
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                if (latestObject.lastModified().compareTo(s3Object.lastModified()) < 1 && s3Object.key().endsWith(".warc.gz")) {
                    latestObject = s3Object;
//                    System.out.println(latestObject.key());
                }
            }
            key = latestObject.key();
        }
        File file = new File(dataPath);
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {

        }
        System.out.println("Using key: "+key);
        System.out.println("Using hostName: "+System.getenv("ELASTIC_SEARCH_HOST"));
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(file));
        s3.close();
        System.out.println("Downloaded");
    }
}


