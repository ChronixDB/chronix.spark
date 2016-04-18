echo "Stopping local Spark cluster"
./spark/sbin/stop-master.sh &
./spark/sbin/start-slaves.sh &