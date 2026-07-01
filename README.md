## Introduction

`libs2client` is a shared library which is supposed to be used by SAS backend (called CAS).
CAS is written in C. It will use `libs2client` to connect to SingleStore(S2) database and run read/write queries. The queries will be processed in parallel in a sense that multiple connections (1 per each partition) will be used to retrieve the data from different database partitions.
`s2_client_extern.h` defines the functions which provide an API to the `libs2client` library. 

## Development

### Local testing

In order to run tests, S2 cluster must be up and running. Access credentials are supplied in the file `test/db_creds.h`.
Then the commands from `test/prepare_s2.sql` must be run, e.g. `mysql -u root -h 127.0.0.1 -P 3306 -ppassword < test/prepare_s2.sql`.
The script `build_and_test.sh` contains several functions used to build the library and run the tests.

- to build the library, run `./build_and_test.sh`
- to build the library and run the main test suite, run `./build_and_test.sh test`. If you see messages starting with `[ERROR]`, something went wrong.
- to build the library and run a particular test, run `./build_and_test.sh <test_name> <print_info>`, where `print_info` is set to 1 to
see some information in shell output, and to 0 to only see status messages: `[SUCCESS]` or `[ERROR]`.

### Building in Docker

`libs2client` can be built inside a RHEL 9 (UBI9) Docker image that provides the compiler toolchain and MariaDB client libraries. The image is defined in `docker/Dockerfile` and tagged `sas-libclient-build:rhel9` by default.

Build the image:

```bash
docker build -t sas-libclient-build:rhel9 -f docker/Dockerfile .
```

On Apple silicon, add `--platform linux/amd64` if you need an amd64 image to match other containers in this repo.

`scripts/build-in-docker.sh` and `scripts/test-in-docker.sh` build this image automatically when it is missing. Use `scripts/test-in-docker.sh --rebuild` to force a fresh image after `docker/Dockerfile` changes.

Build the library inside the image:

```bash
./scripts/build-in-docker.sh
```

This mounts the repository at `/workspace` and runs `./build_and_test.sh` with no arguments (library build only). Set the CMake build type with `CMAKE_BUILD_TYPE` or `--cmake-build-type` (default: `RelWithDebInfo`):

```bash
./scripts/build-in-docker.sh --cmake-build-type Debug
CMAKE_BUILD_TYPE=Release ./scripts/build-in-docker.sh
```

To rebuild the image manually after `docker/Dockerfile` changes:

```bash
docker build -t sas-libclient-build:rhel9 -f docker/Dockerfile .
```

### Testing in Docker

Docker scripts build and test inside a RHEL 9 container against a SingleStore dev database. This avoids setting up SingleStore and build dependencies on the host.

- `./scripts/build-in-docker.sh` — build the library only
- `./scripts/s2-docker.sh` — start and manage the SingleStore test container (`start`, `stop`, `status`, `prepare`)
- `./scripts/test-in-docker.sh` — build and run tests against an already-running SingleStore container

Typical workflow: start the test database once, then run tests as needed:

```bash
./scripts/s2-docker.sh start

./scripts/test-in-docker.sh test
./scripts/test-in-docker.sh parallel_read 1
./scripts/test-in-docker.sh --rebuild testcc

./scripts/s2-docker.sh stop
```

To have the test script start and stop SingleStore for a one-off run:

```bash
./scripts/test-in-docker.sh --manage-s2 test
```

Use `--manage-s2 --keep-s2` to start SingleStore but leave it running after tests finish.

Debug a single test with gdb inside the test container (rebuild the image once after Dockerfile changes):

```bash
./scripts/test-in-docker.sh --rebuild --gdb parallel_read 1
./scripts/test-in-docker.sh --gdb write 1
```

Database credentials for tests come from `test/db_creds.h`. When using `test-in-docker.sh`, `host` and `password` are patched for the test run from `S2_HOST` and `ROOT_PASSWORD` (default password: `password`) and then restored. Edit `test/db_creds.h` directly for other settings such as `db`, `user`, `ma_port`, and `ssl_ca`.

Use `./scripts/test-in-docker.sh --help` or `./scripts/s2-docker.sh --help` for all options and environment variables.

## Releasing

Pushing a semver tag (`v*.*.*`) triggers the [Release](.github/workflows/release.yml) GitHub Action. It runs the full test suite first, then builds `libs2client` in the RHEL 9 Docker image with `Release` CMake settings, packages the artifacts, and publishes a GitHub release.

Create and push a release tag:

```bash
git tag v1.0.0
git push origin v1.0.0
```

Each release includes:

- `libs2client-<version>-linux-amd64.tar.gz` — archive of all files below
- `libs2client.so`
- `s2_client_extern.h`, `chunk_extern.h`, `hdat_write_extern.h`

To build the same artifacts locally:

```bash
./scripts/build-in-docker.sh --cmake-build-type Release share
```

Output is written to `build/share/`.

Pull requests and pushes to `master` run the [Test](.github/workflows/test.yml) workflow, which executes the C and C++ test suites in Docker against SingleStore. The [Release](.github/workflows/release.yml) workflow runs the same tests before publishing artifacts.

## Workflow

CAS will have multiple *workers* (kubernetes pods which are supposedly colocated with S2 aggregators, but not necessarily) and each of them will establish a connection to S2 using `S2ClientInit(...)`.

### Parallel read (single pass, or streaming)
1. One designated worker calls `ParallelReadInit(...)` function to initiate a request to S2 providing, among other parameters:
  - `selectQuery` - an SQL query represented as string (`char*`).
  - `resultTableName` - a name of the table which S2 will use to store the results of the query execution
2. `ParallelReadInit` performs the following steps:

    1. Transform the query to `CREATE RESULT TABLE {resultTableName} AS {selectQuery}`.
    2. Send the transformed query to S2.

3. The results of `selectQuery` will ne streamed to CAS using a queue `ChunkQueue` which is a thread-safe queue of `(partitionId, Chunk)` pairs.
The queue is initialized in each CAS worker using `ParallelReadGetQueue(...)` function.

4. When `ParallelReadGetQueue(...)` has been called by a CAS worker, `libs2client` spawns a number of readers equal to `workerPartitions`.

    - Each reader creates its own connection to S2
    - Each reader reads from its own partition by calling `SELECT * FROM resultTableName WHERE partition_id = {assigned_partition}` on its connection
    - The rows that are streamed from `MYSQL_STMT` object are transformed from `MYSQL_BIND` to SAS-HDAT (SuperChunk) format and grouped in chunks
    - Each reader appends the results of the read to the worker's `chunkQueue`

5. Each CAS worker calls `bool GetNextChunk(...)` in an infinite loop. Memory for `chunk->m_ptr` data is allocated by `libs2client`. The pointer to it is copied to `chunk`. To free the memory, `ChunkFree(chunk)` must be called.
