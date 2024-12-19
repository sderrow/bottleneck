#!/usr/bin/env bash

set -e

source .env

echo 'ioredis tests'
DATASTORE=ioredis npm test

echo 'NodeRedis tests'
DATASTORE=redis npm test

echo 'Local tests'
npm test
