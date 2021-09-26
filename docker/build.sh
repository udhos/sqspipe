#!/bin/bash

version=$(go run ./sqspipe -version | cut -d' ' -f2 | cut -d'=' -f2)
echo sqspipe version=$version
docker build -t udhos/sqspipe:$version -f docker/Dockerfile .
