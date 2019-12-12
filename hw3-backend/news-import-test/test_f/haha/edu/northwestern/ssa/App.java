package edu.northwestern.ssa;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.jsoup.Jsoup;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.jsoup.nodes.Document;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class App {
//    private  String dataPath = "/Users/weijianli/ssa/ssa-skeleton/data/data.warc.gz";
    private static String serviceName = "es";

    public static void main(String[] args) throws IOException {
        String dataPath = "/Users/weijianli/ssa/ssa-skeleton/data/data.warc.gz";
        System.getenv("AWS_SECRET_ACCESS_KEY");
        System.getenv("AWS_ACCESS_KEY_ID");
        System.getenv("ELASTIC_SEARCH_HOST");
        System.getenv("ELASTIC_SEARCH_INDEX");
        System.getenv("COMMON_CRAWL_FILENAME");


//        dataDownload();

        ElasticSearch elasticSearch = new ElasticSearch(serviceName);
        elasticSearch.createElasticIndex();

        File f = new File(dataPath);
        ArchiveReader archiveReader = WARCReaderFactory.get(f);
        Iterator<ArchiveRecord> it = archiveReader.iterator();
        ArchiveRecord record = it.next();
        ExecutorService executorService = Executors.newFixedThreadPool(16);
        int i=0;
        while (it.hasNext()) {
            record = it.next();
            InputStream inputStream = record;
            String docUrl = record.getHeader().getUrl();
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
//            for (char c : recordChars) {
//                System.out.println(c);
//            }
//            for (int k=0; k<inputStream.available(); k++) {
//                int x = inputStream.read();
//                if (x == 0) continue;
//                recordChars[n] = (char) x;
//            }
//
//            byte[] rawContent = new byte[record.available()];
//
//            int recordInt = record.read(rawContent);
//            for (int j=0; j < rawContent.length; j++) {
//                if (rawContent[j] == 0) {
//                    rawContent[j] = (byte) ' ';
//                }
//            }
//
//            String content = new String(rawContent);
            String content = new String(recordChars);

            if (recordChars[0] != 'G') {
                String[] contentTmp = (content.split("\\r\\n\\r\\n", 2));
                content = contentTmp[1];
                Document htmlDoc = Jsoup.parse(content);

                Runnable runnableTask = () -> {
                    try {
                        // create the index for this document on ElasticSearch
                        ElasticSearch elasticSearch1 = new ElasticSearch(serviceName);
                        elasticSearch1.createDocument(htmlDoc, docUrl);
//                        System.out.println(i++);
                    } catch (IOException e) {

                    }
                };
                executorService.submit(runnableTask);


//                System.out.println("title:  " + htmlTitle);
//                System.out.println("Content: " + htmlText.substring(0, Math.min(5000, htmlText.length())));
//                System.out.println((htmlText.length() > 5000 ? "..." : ""));
//                System.out.println("=-=-=-=-=-=-=-=-=");
//                if (i++ > 10) break;
            }

        }

    }

    private static void dataDownload() {
        String dataPath = "/Users/weijianli/ssa/ssa-skeleton/data/data.warc.gz";
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

        String bucket = "commoncrawl";
        String key = System.getenv("COMMON_CRAWL_FILENAME");
        // if no filename specified, get the latest one
        if (key.isEmpty()) {
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").build();
            ListObjectsV2Response listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
            S3Object latestObject = listObjectsV2Response.contents().get(0);
            while (listObjectsV2Response.isTruncated()) {
                if (!listObjectsV2Response.contents().get(999).key().endsWith(".warc.gz")) {
//                    System.out.println(listObjectsV2Response.contents().get(999).key());
                    listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).prefix("crawl-data/CC-NEWS/").continuationToken(listObjectsV2Response.nextContinuationToken()).build();
                    listObjectsV2Response = s3.listObjectsV2(listObjectsV2Request);
                    continue;
                }
                int i = 1;
                while (!latestObject.key().endsWith(".warc.gz") && i < 1000) {
                    latestObject = listObjectsV2Response.contents().get(i);
                    System.out.println(latestObject.key());
                    i++;
                }
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    if (latestObject.lastModified().compareTo(s3Object.lastModified()) < 1 && s3Object.key().endsWith(".warc.gz")) {
                        latestObject = s3Object;
//                        System.out.println(latestObject.key());
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
        s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build(),
                ResponseTransformer.toFile(file));
        s3.close();
    }
}
