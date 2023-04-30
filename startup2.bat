::Copy csv from hadoop_namenode folder to visualization app
echo "Transferring data from volume to visualization app..."
docker cp hadoop_namenode/output viscontainer:/usr/local/
