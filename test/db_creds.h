/*
 * Copyright 2021-2026 SingleStore, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TEST_DB_CREDS_H
#define TEST_DB_CREDS_H

typedef struct DBCreds
{
    const char* host;
    const int ma_port;
    const char* db;
    const char* user;
    const char* password;
    const char* ssl_ca;
} DBCreds;

DBCreds db_creds =
    {
        .host = "127.0.0.1",
        .ma_port = 3306,
        .db = "testdb",
        .user = "root",
        .password = "password",
        .ssl_ca = NULL,
};

// DBCreds db_creds =
//     {
//         .host = "127.0.0.1",
//         .ma_port = 3306,
//         .db = "testdb",
//         .user = "ssl_user",
//         .password = "p",
//         .ssl_ca = "/home/pmishchenko-ua/certs/ca-cert.pem"
// };

#endif  // TEST_DB_CREDS_H
