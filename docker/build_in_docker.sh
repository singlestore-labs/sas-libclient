docker rm build-libs2client
docker build -t build-libs2client .
cd ..
docker run -v .:/root/sas --name build-libs2client build-libs2client
