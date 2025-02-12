#!/bin/bash
if [ $SOCKET_TYPE = "unix" ]
then
   curl --silent --fail --unix-socket /run/dexapi/dexapi.socket http://localhost/info
else
   curl --silent --fail http://127.0.0.1:8080/info
fi
