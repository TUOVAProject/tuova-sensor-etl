#!/bin/bash

rm -rf ./build
mkdir ./build

cp ./config.json ./pyproject.toml ./build
cp -r ./tuova_sensors ./build
rm -rf ./build/tuova_sensors/__pycache__
