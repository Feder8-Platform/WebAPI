# Feder8 WebAPI 

The source code of Feder8 WebAPI is based on [OHDSI/WebAPI_v2.15.1](https://github.com/OHDSI/WebAPI/tree/v2.15.1) with some modifications to ensure the correct connection with feder8-studio (keycloak), making database connection, database url, database schema and security settings configurable, adding a healthcheck in Dockerfile and using correct path for WebAPI service:

- Dockerfile: healthcheck added and using java 11
    - line 1 to 8: golang image used for the healthcheck 
    - line 41: using java 11 as runtime image
    - line 45 to 47: adding files in runtime stage for healthcheck execution and healthcheck itself

- pom.xml/sample_setting.xml: database connection, database url, database schema and security settings configurable

- src/main/java/org/ohdsi/webapi/common/sensitiveinfo/AbstractAdminService.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - line 50, including admin role created by us 

- src/main/java/org/ohdsi/webapi/shiro/PermissionManager.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - removeUserFromAllRole function kept from previous feder8 webapi 
    - from line 203 to 208, ensuring ADMIN_LOGIN included 

- src/main/java/org/ohdsi/webapi/shiro/filters/UpdateAccessToken.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - line 71, using the username instead of the email for the connection (username store in webapi tables on the db instead of email)

- src/main/java/org/ohdsi/webapi/shiro/management/AtlasRegularSecurity.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - line 278, kept using UPDATE_ATLAS_ROLE_FROM_TOKEN filter 
    - line 333 to 342, keeping AuthorizationGenerator from previous feder8 webapi
    - line 394, keeping Feder8CallbackLogic as a callback filter

- src/main/java/org/ohdsi/webapi/shiro/management/AtlasSecurity.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - line 80, keeing "Atlas users" as a default Role

- src/main/java/org/ohdsi/webapi/shiro/management/FilterTemplates.java: ensure correct connection handled by Feder8-Studio (keycloak)
    - line 29, defining UPDATE_ATLAS_ROLE_FROM_TOKEN filter 

- src/main/resources/application.properties: using correct path for WebAPI service
    - line 90, server.context-path set to /webapi instead of /WebAPI

# OHDSI WebAPI

OHDSI WebAPI contains all OHDSI RESTful services that can be called from OHDSI applications.

## Features

- Provides a centralized API for working with 1 or more databases converted to the [Common Data Model](https://github.com/OHDSI/CommonDataModel) (CDM) v5.
- Searching the OMOP standardized vocabularies for medical concepts and constructing concept sets.
- Defining cohort definitions for use in identifying patient populations.
- Characterizing cohorts
- Computing incidence rates
- Retrieve patient profiles
- Design population level estimation and patient level prediction studies

## Technology

OHDSI WebAPI is a Java 8 web application that utilizes a PostgreSQL database for storage.

## API Documentation

The API Documentation is found at [http://webapidoc.ohdsi.org/](http://webapidoc.ohdsi.org/)

## System Requirements & Installation

Documentation can be found a the [Web API Installation Guide](https://github.com/OHDSI/WebAPI/wiki) which covers the system requirements and installation instructions.

## SAML Auth support

The following parameters are used:

- `security.saml.idpMetadataLocation=classpath:saml/dev/idp-metadata.xml` - path to metadata used by identity provider
- `security.saml.metadataLocation=saml/dev/sp-metadata.xml` - service provider metadata path
- `security.saml.keyManager.keyStoreFile=classpath:saml/samlKeystore.jks` - path to keystore
- `security.saml.keyManager.storePassword=nalle123` - keystore password
- `security.saml.keyManager.passwords.arachnenetwork=nalle123` - private key password
- `security.saml.keyManager.defaultKey=apollo` - keystore alias
- `security.saml.sloUrl=https://localhost:8443/cas/logout` - identity provider logout URL
- `security.saml.callbackUrl=http://localhost:8080/WebAPI/user/saml/callback` - URL called from identity provider after login

Sample idp metadata and sp metadata config files for okta:
- `saml/dev/idp-metadata-okta.xml`
- `saml/dev/sp-metadata-okta.xml`

## Managing auth providers

The following parameters are used to enable/disable certain provider:

- `security.auth.windows.enabled`
- `security.auth.kerberos.enabled`
- `security.auth.openid.enabled`
- `security.auth.facebook.enabled`
- `security.auth.github.enabled`
- `security.auth.google.enabled`
- `security.auth.jdbc.enabled`
- `security.auth.ldap.enabled`
- `security.auth.ad.enabled`
- `security.auth.cas.enabled`

Acceptable values are `true` and `false`

## Geospatial support

Instructions can be found at [webapi-component-geospatial](https://github.com/OHDSI/webapi-component-geospatial)

## Testing

It was chosen to use embedded PG instead of H2 for unit tests since H2 doesn't support window functions, `md5` function, HEX to BIT conversion, `setval`, `set datestyle`, CTAS + CTE.

## Support

- Developer questions/comments/feedback: [OHDSI forum](http://forums.ohdsi.org/c/developers)
- We use the [GitHub issue tracker](https://github.com/OHDSI/WebAPI/issues) for all bugs/issues/enhancements.

## Contribution

### Versioning

- WebAPI follows [Semantic versioning](https://semver.org/);
- Only Non-SNAPSHOT dependencies should be presented in POM.xml on release branches/tags.

## License
OHDSI WebAPI is licensed under Apache License 2.0