#!/usr/bin/env bash
set -ex

VERSION=testing
TAG=$VERSION

touch webapi.env

echo "FEDER8_WEBAPI_SECURE=false" >> webapi.env
echo "FEDER8_WEBAPI_CENTRAL=false" >> webapi.env
echo "WEBAPI_USER=feder8_admin" >> webapi.env
echo "WEBAPI_USER_PW=feder8_admin" >> webapi.env

docker run \
--rm \
--name webapi \
-p 8080:8080 \
-p 54322:54322 \
-v shared:/var/lib/shared \
--env-file webapi.env \
--network feder8-net \
feder8/webapi:$TAG

rm -rf webapi.env

