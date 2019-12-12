package edu.northwestern.ssa;

import org.archive.io.ArchiveRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class MyCallable implements Callable {
    //        private final String serviceName;
//        private final Document document;
//        private final String url;
    private String content;
    private String docUrl;
    MyCallable(String content, String docUrl) {
//            this.serviceName = serviceName;
//            this.document = document;
//            this.url = url;
        this.content = content;
        this.docUrl = docUrl;
//        this.record = record;
    }
    @Override
    public List<Object> call() {
        Document htmlDoc = Jsoup.parse(content);
        List<Object> doc = new ArrayList<>();
        doc.add(htmlDoc);
        doc.add(docUrl);
        return doc;
    }
}
