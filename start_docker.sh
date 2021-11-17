BASEDIR=$(dirname "$0")
cd $BASEDIR/docker_data

docker-compose stop
docker-compose -f ../docker-hive/docker-compose.yml -f ./docker-compose.extras.yml config > ./docker-compose.yml
docker-compose up -d