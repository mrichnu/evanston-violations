#!/bin/sh

rm build_tweeter/*.py
rm build_tweeter/*.zip
cp tweet.py build_tweeter/
cd build_tweeter && zip -r9 build.zip *
echo "Done"
