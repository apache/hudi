# Create host mount directory and copy
mkdir -p /tmp/hadoop_name
mkdir -p /tmp/hadoop_data

WS_ROOT=`dirname $PWD`
# restart cluster
HUDI_WS=${WS_ROOT} docker-compose -f compose/docker-compose_hadoop284_hive233_spark231.yml down
HUDI_WS=${WS_ROOT} docker-compose -f compose/docker-compose_hadoop284_hive233_spark231.yml pull
rm -rf /tmp/hadoop_data/*
rm -rf /tmp/hadoop_name/*
sleep 5
HUDI_WS=${WS_ROOT} docker-compose -f compose/docker-compose_hadoop284_hive233_spark231.yml up -d
sleep 15

docker exec -it adhoc-1 /bin/bash /var/hoodie/ws/docker/demo/setup_demo_container.sh
docker exec -it adhoc-2 /bin/bash /var/hoodie/ws/docker/demo/setup_demo_container.sh
