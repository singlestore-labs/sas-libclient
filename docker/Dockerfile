FROM almalinux:8

RUN yum update -y && \
    yum install -y gcc gcc-c++ cmake
RUN ln -s /usr/lib64/libz.so.1 /usr/lib64/libz.so && \
    ln -s /usr/lib64/libssl.so.1.1 /usr/lib64/libssl.so && \
    ln -s /usr/lib64/libcrypto.so.1.1 /usr/lib64/libcrypto.so

WORKDIR /root/sas
ENTRYPOINT ["/root/sas/build_and_test.sh", "share"]
