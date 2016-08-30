#!/bin/sh

rm build/*.py
rm build/*.zip
cp violations.py build/
cp lambda.py build/
cd build && zip -r9 build.zip *
echo "Done"
