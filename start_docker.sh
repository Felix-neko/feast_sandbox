BASEDIR=$(dirname "$0")
cd $BASEDIR/docker_data

docker-compose stop
docker-compose -f ../docker-hive/docker-compose.yml -f ./docker-compose.extras.yml config > ./docker-compose.yml
CURRENT_UID=$(id -u):$(id -g) docker-compose up -d