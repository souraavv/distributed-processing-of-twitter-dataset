• Distributed Processing of Twitter Dataset

◦ Batch Processing Designed a scalable and fault-tolerant word count application using Redis and Celery
- Achieved 3x speedup over serial processing of tweets
◦ Stream Processing Designed a load balanced tweets processing pipeline using Redis streams and Celery workers
- Achieved processing speed of 31k words/sec using 8 worker threads
◦ Eventual Consistency Designed a highly available, partition tolerant distributed storage system
- Strong eventual consistency achieved for 3 Million+ words within 50 seconds after network partition recovery
