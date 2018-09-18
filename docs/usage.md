Run spark jobs on cluster assuming necessary packages have been installed on cluster:
    1. start cluster
            source ~/.bash_profile
            peg start <cluster-name>
    2. start hadoop and spark on the cluster
            peg service <cluster-name> hadoop start
            peg service <cluster-name> spark start
    3. peg ssh <cluster-name> <master-node-number>
    4. pip install pyspark if not installed
    5. cd /usr/local/spark/bin
    6. move the python scripts to this folder
    7. possibly use the spark-shell 'pyspark' to evaluate the code first
    8. spark-submit --master spark://ip-10-0-0-6:7077 \
                    --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
                    --executor-memory 4G \
                    sparkbatch_CrawlerFinder.py

        format:
        spark-submit --master spark://<cluster-master-node-private-IP:7077>
                    --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0
                    --executor-memory 4G
                    python_code.py argument1 argument2 ...s
    9. use spark web UI to monitor the jobs
            open a browser, enter url: 'mater-node public dns:4040' or 8080
    10. make sure the aws ec2 security group inbound policy allows visits, especially ports
    7077,4040,8080....



connect to rds PostgreSQL database:
    1. pip install psycopg2
    2. find all the connection parameters: master-username, password, endpoint, port (5432)
    3. make sure security group allows visits
    4. remember to commit after create, update, insert ...
