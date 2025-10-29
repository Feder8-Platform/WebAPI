#!/usr/bin/env bash
set -eux

VERSION=1.0.0
TAG=2.15.1-$VERSION

docker tag feder8/webapi:latest $THERAPEUTIC_AREA_URL/$THERAPEUTIC_AREA/webapi:$TAG
docker push $THERAPEUTIC_AREA_URL/$THERAPEUTIC_AREA/webapi:$TAG
