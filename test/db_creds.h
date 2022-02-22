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

// DBCreds db_creds =
//     {
//         .host = "127.0.0.1",
//         .ma_port = 3306,
//         .db = "testdb",
//         .user = "root",
//         .password = "p",
// };

DBCreds db_creds =
    {
        .host = "127.0.0.1",
        .ma_port = 3306,
        .db = "testdb",
        .user = "ssl_user",
        .password = "p",
        .ssl_ca = "/home/pmishchenko-ua/certs/ca-cert.pem"
};

#endif  // TEST_DB_CREDS_H
