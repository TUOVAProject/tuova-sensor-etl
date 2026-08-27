#!/bin/bash

rm -rf ./build
mkdir ./build

cp ./.env ./pyproject.toml ./build
cp -r ./tuova_sensors ./build
rm -rf ./build/tuova_sensors/__pycache__
