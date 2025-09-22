#!/bin/bash
curl -X 'POST' \
  'http://localhost:8080/v2/hooks' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSDgxOUczWk1LNURDOVE1UEQxOE45U1hCIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjE3OTA3MjY0MDAsInRpZCI6IjAxSzVIMDY2WENLRk42RFlQMVFIOTkxSEhUIn0.bs7OQlJt-I549QAOh3Py0gVR8Zi6c4Bo-je7kOwNFbbsG543XbRzR5MkmnrB7UqHWd0xz4EdNMd2dsZcRO9_CQ' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "example",
  "trigger": {
    "triggerType": "TRIGGER_TYPE_LABEL_ADDED",
    "filters": [
      {
        "name": ".*"
      }
    ]
  },
  "hook": {
    "externalHook": {
      "url": "http://0.0.0.0:1234",
      "method": "METHOD_POST"
    }
  },
  "timeout": "1884509437",
  "projectIds": [
    "01K5H08Z6PP8CKQ2EAWJW56NM6"
  ],
  "description": "abcd converter"
}'
