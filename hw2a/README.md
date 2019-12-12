Configuration
=============
The following environment variables must be set:
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* ELASTIC_SEARCH_HOST
* ELASTIC_SEARCH_INDEX

To Run
======
```
$ mvn clean package tomcat7:run
```

References
==========
https://www.mkyong.com/webservices/jax-rs/jersey-hello-world-example/


Test Case
==========
http://localhost:8080/api/search?query=Apple%20Store&language=en&count=4
http://localhost:8080/api/search?query=Northwestern%20University&language=en&count=4&offset=100&date="2019-10-16"
http://localhost:8080/api/search?query=Apple%20Store&language=en&count=4&offset=56
http://localhost:8080/api/search?query=evanston&language=en&count=4&offset=58
http://localhost:8080/api/search?query=evanston&date="2019-09-16"
