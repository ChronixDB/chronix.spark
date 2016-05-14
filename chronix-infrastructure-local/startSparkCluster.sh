echo "CHRONIX SPARK ###############################################################"
echo "Starting local Spark cluster ..."
./spark/sbin/start-master.sh --host localhost --port 8987 --webui-port 8988
./spark/sbin/start-slave.sh spark://127.0.0.1:8987