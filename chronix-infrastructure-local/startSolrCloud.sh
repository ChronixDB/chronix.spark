echo "CHRONIX SPARK ###############################################################"
echo "Starting Chronix Spark local Cluster (basically Solr Cloud and ZooKeeper) ..."
./solr-cloud/solr/bin/solr start -cloud -p 8983 -s ./solr-cloud/data/node1/solr
./solr-cloud/solr/bin/solr start -cloud -p 7574 -s ./solr-cloud/data/node2/solr -z localhost:9983
