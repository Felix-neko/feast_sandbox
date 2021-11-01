BASEDIR=$(dirname "$0")
cd $BASEDIR
cd ./docker-hive
docker-compose stop
docker-compose up -d

cd ../docker-redis
#docker stop redis-sandbox
docker run --name redis-sandbox -p 127.0.0.1:6379:6379 -v $BASEDIR/docker-redis/data:/data -d redis redis-server --save 60 1 --loglevel warning