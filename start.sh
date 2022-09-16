#! /bin/sh

TMP="$(dirname $0)"
DIR="$(realpath $TMP)"

echo $DIR

#sudo systemctl stop couchbase-server
cd $DIR
docker compose down
docker compose up