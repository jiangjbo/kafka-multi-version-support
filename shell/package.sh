#!/usr/bin/env bash

CURRENT_PATH=$(cd `dirname $0`; pwd)
cd ../
mvn package -e -U -DskipTests
