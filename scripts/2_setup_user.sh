#!/bin/bash

curl -i -X POST \
   -H "Content-Type:application/json" \
   -d \
'{
  "email":"test@gmail.com",
  "password":"fcfcfc",
  "org_name":"fc",
  "name":"Test Account"
}' \
 'http://localhost:5000/api/setup'
