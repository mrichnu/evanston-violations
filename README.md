# evanston-violations
A project to download and view health code violation data from the city of Evanston

# Building
To build the zip file to upload to lambda:
```
cp lambda.py build/
cp violations.py build/
cd build
rm build.zip
zip -r9 build.zip *
```
