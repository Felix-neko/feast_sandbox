BASEDIR=$(dirname "$0")
cd $BASEDIR/docker_data
yes | cp -rf ../docker-hive/hadoop-hive.env ../docker_data/hadoop-hive.env
docker-compose -f ../docker-hive/docker-compose.yml -f ./docker-compose.extras.yml config > ./docker-compose.yml
docker-compose stop
CURRENT_UID=$(id -u):$(id -g) docker-compose up -d