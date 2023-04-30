::Remove containers
docker-compose -f (dockercompose directory)\docker-compose.yml down

::Clean out docker
docker rm -f $(docker ps -a -q)
docker volume rm $(docker volume ls -q)