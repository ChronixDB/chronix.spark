echo "CHRONIX SPARK ###############################################################"
echo "Stopping local Spark cluster ..."
./spark/sbin/stop-master.sh &
./spark/sbin/stop-slaves.sh &