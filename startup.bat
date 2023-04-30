::start building and running containers
echo "Starting up containers..."
docker-compose -f D:\Zsfyp\Changes\FypApp\docker-compose.yml up -d
::docker-compose -f (dockercompose directory)\docker-compose.yml up -d
echo "Start up complete."

::Get data from API to store in container's /app/data
echo "Getting weather data..."
docker exec -it sparkcontainer python3 main.py
echo "Weather data get. Stored in sparkcontainer /app/data"

::Use container's volume to transfer data between containers
:: - transfer from sparkcontainer to hadoop_namenode volume that was specified in compose file
echo "Transferring weather data to volume..."
docker cp sparkcontainer:/app/ hadoop_namenode/   

:: - transfer from hadoop_namenode volume to /tmp in namenode
echo "Transferring weather data to namenode..."
docker cp hadoop_namenode/data namenode:/tmp    

:: -put data from /tmp into hdfs
echo "Transferring weather data to hdfs..."
docker exec -it namenode hdfs dfs -put /tmp/data/ /data


::Run script that processes the data (data will be in /usr/local/output)
echo "Now cleaning processing weather data..."
docker exec -it sparkcontainer spark-submit process.py
echo "Processing done. Stored in /usr/local/output"

::Store processed csv in hadoop_namenode
echo "Transferring cleaned data to volume..."
docker cp sparkcontainer:/usr/local/output hadoop_namenode/

::Run new container of visualization app
echo "Running visualization app..."
docker run --name viscontainer -p 8501:8501 fypapp-visualization


:: val df = spark.read.csv("hdfs://namenode:9000/data/data_australia.csv")
:: To run this file,  C:\Users\USER\Fyapp\Changes\Fypapp\automate.bat
::docker cp C:\Users\USER\Downloads\combined_csv.csv namenode:/tmp

