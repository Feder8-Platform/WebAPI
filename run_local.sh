#!/usr/bin/env bash
set -ex

VERSION=testing
TAG=$VERSION

touch webapi.env

echo "FEDER8_WEBAPI_SECURE=false" >> webapi.env
echo "FEDER8_WEBAPI_CENTRAL=false" >> webapi.env
echo "FEDER8_WEBAPI_OIDC_REDIRECT_URL_UI=http://localhost:80/atlas/#/welcome" >> webapi.env
echo "FEDER8_WEBAPI_OIDC_REDIRECT_URL_API=http://localhost:80/webapi/user/oauth/callback" >> webapi.env
echo "FEDER8_WEBAPI_OIDC_SECRET=secret" >> webapi.env
echo "KEYCLOAK_DISCOVERY_URI=http://keycloak:8080/auth/realms/feder8/.well-known/openid-configuration" >> webapi.env
docker run \
--rm \
--name webapi \
-p 8080:8080 \
-p 54322:54322 \
-v shared:/var/lib/shared \
--env-file webapi.env \
--network feder8-net \
--user 101:101 \
feder8/webapi:$TAG 

rm -rf webapi.env