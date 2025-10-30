#!/bin/bash

TOKEN=""              # Bearer token with appropriate permissions
PROJECT=""            # The project ID of the project to which the hook will be attached
TOKEN_EXPIRATION=""   # Note this is a unix timestamp in milliseconds

curl -X 'POST' \
    'http://localhost:8080/v2/hooks' \
    -H 'accept: application/json' \
    -H 'Authorization: Bearer ${TOKEN}' \
    -H 'Content-Type: application/json' \
    -d '{
    "name": "ABCD-to-BioSchema",
    "trigger": {
      "triggerType": "TRIGGER_TYPE_OBJECT_FINISHED",
      "filters": [
        {
          "keyValue": {
            "key": "^ABCD$",
            "value": ".*",
            "variant": "KEY_VALUE_VARIANT_LABEL"
          }
        }
      ]
    },
    "hook": {
      "externalHook": {
        "url": "http://abcd2bioschema-service.aruna.svc.cluster.local:5000/transform/url",
        "method": "METHOD_POST"
      }
    },
    "timeout": "${TOKEN_EXPIRATION}",
    "projectIds": [
      "${PROJECT}"
    ],
    "description": "Transforms ABCD metadata into BioSchema."
}'
