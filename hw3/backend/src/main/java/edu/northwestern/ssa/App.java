package edu.northwestern.ssa;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.time.Instant;

import org.jsoup.nodes.Document;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;


public class App implements RequestHandler<Map<String, Object>, Void>{
    public Void handleRequest(Map<String, Object> input, Context context) {
        try {
            String dataPath = "data.warc.gz";
            System.getenv("AWS_SECRET_ACCESS_KEY");
            System.getenv("AWS_ACCESS_KEY_ID");
            System.getenv("ELASTIC_SEARCH_HOST");
            System.getenv("ELASTIC_SEARCH_INDEX");
            System.getenv("COMMON_CRAWL_FILENAME");

            S3Client s3 = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .build();

            String bucket = "commoncrawl";
            String key = System.getenv("COMMON_CRAWL_FILENAME");
            String lastModifed = "latestObject.lastModified()";
            System.out.println(key);
            // if no filename specified, get the latest one
            if (key == null || key.isEmpty()) {
                ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").build();
                ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                S3Object latestObject = listObjectsV2Response.contents().get(0);
                while (listObjectsV2Response.isTruncated()) {
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
                    }
                }
                key = latestObject.key();
                lastModifed = latestObject.lastModified().toString();
            }
            System.out.println("Using key: "+key);
            System.out.println("Using hostName: "+System.getenv("ELASTIC_SEARCH_HOST"));
            System.out.println("Current directory is: "+System.getProperty("user.dir"));
            System.out.println("lambda root path: "+System.getenv("LAMBDA_TASK_ROOT"));

            String bucket_name = "wlk5936-indexfilename";
            System.out.format("Objects in S3 bucket %s:\n", bucket_name);
            ListObjectsV2Request listObjectsV2Request2 = ListObjectsV2Request.builder().bucket(bucket_name).build();
            ListObjectsV2Response listObjectsV2Response2 = s3.listObjectsV2(listObjectsV2Request2);
            String keyOnS3 = "";
//            File f = new File("/var/task"); // current directory
//            File[] files = f.listFiles();
//            for (File file : files) {
//                if (file.isDirectory()) {
//                    System.out.print("directory:");
//                } else {
//                    System.out.print("     file:");
//                }
//                System.out.println(file.getCanonicalPath());
//            }
            System.out.println("latest lastModified: "+lastModifed);
            if (!listObjectsV2Response2.contents().isEmpty()) {
                S3Object latestObjectOnS3 = listObjectsV2Response2.contents().get(0);
                keyOnS3 = latestObjectOnS3.lastModified().toString();
                System.out.println("latestKey: "+lastModifed);
                System.out.println("keyOnS3: "+keyOnS3);
            }
            if (!lastModifed.equals(keyOnS3)) {
                try {
                    s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket_name).key(keyOnS3).build());
                } catch (Exception e) {
                    System.err.println(e.toString());
                    System.exit(1);
                }
                try {
                    ClassLoader classLoader = getClass().getClassLoader();

                    File keyFile = new File("/var/task/edu/northwestern/ssa/latestKey");
                    s3.putObject(PutObjectRequest.builder().bucket(bucket_name).key(lastModifed).build(), RequestBody.fromFile(keyFile));
                } catch (Exception e) {
                    System.err.println(e.toString());
                    System.exit(1);
                }

                InputStream s3ObjectResponseInputStream = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                        ResponseTransformer.toInputStream());
//        BufferedInputStream bufferedInputStream = new BufferedInputStream(s3ObjectResponseInputStream);

                System.out.println("Started streaming");

                ElasticSearch elasticSearch = new ElasticSearch(serviceName);
                elasticSearch.createElasticIndex();

//        ArchiveReader archiveReader = WARCReaderFactory.get("latest_warc", s3ObjectResponseInputStream, true);

                ArchiveReader archiveReader = WARCReaderFactory.get("latest_warc.gz", new BufferedInputStream(s3ObjectResponseInputStream), true);
//        for (ArchiveRecord record: archiveReader) {
//            System.out.println(record.getHeader());
//        }

                Iterator<ArchiveRecord> it = archiveReader.iterator();
                ArchiveRecord record = it.next();
                int i=0;
                int sendCount = 0;
                List<List<Object>> listOfDocs = new ArrayList<>();
                ArrayList<String> listOfDocStrings = new ArrayList<>();
                while (it.hasNext()) {
                    record = it.next();
                    String docUrl = record.getHeader().getUrl();
                    String docDate = record.getHeader().getDate().split("T")[0];
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
                        sendCount++;
                        String jsonDoc = createJsonDoc(htmlDoc, docUrl, docDate);
                        listOfDocStrings.add(jsonDoc);

                    }

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
                s3ObjectResponseInputStream.close();
                s3ObjectResponseInputStream.close();
                s3.close();

            }




        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //    private  String dataPath = "/Users/weijianli/ssa/ssa-skeleton/data/data.warc.gz";
    private static String serviceName = "es";

    private static String createJsonDoc(Document doc, String url, String date) {
        String indexingMeta = "{ \"index\" : {\"_index\" : \"" + System.getenv("ELASTIC_SEARCH_INDEX") + "\"}}\n";
        StringBuilder jsonBody = new StringBuilder();
        String contentToIndex;
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("title", doc.title());
        jsonObject.put("txt", doc.text());
        jsonObject.put("url", url);
        jsonObject.put("date", date);
        jsonObject.put("language", doc.select("html").first().attr("lang"));
        contentToIndex = jsonObject.toString()+"\n";
        jsonBody.append(indexingMeta);
        jsonBody.append(contentToIndex);
        return jsonBody.toString();
    }

    public static void main(String[] args) throws IOException {

    }

}


