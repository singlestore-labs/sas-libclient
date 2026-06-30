FROM registry.access.redhat.com/ubi9/ubi:latest

RUN dnf install -y \
    gcc \
    gcc-c++ \
    cmake \
    make \
    jansson \
    openssl-devel \
    zlib-devel \
    mariadb \
    mariadb-connector-c-devel \
    git \
    gdb \
    && dnf clean all

# The vendored libmariadb requires SSL; the system connector works with the dev image.
ENV MARIADB_INCLUDE_DIR=/usr/include/mysql
ENV MARIADB_LIB_DIR=/usr/lib64

WORKDIR /workspace
