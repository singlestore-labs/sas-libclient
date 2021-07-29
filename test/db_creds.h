#ifndef TEST_DB_CREDS_H
#define TEST_DB_CREDS_H

typedef struct DBCreds
{
    const char* host;
    const int ma_port;
    const char* db;
    const char* user;
    const char* password;
} DBCreds;

DBCreds db_creds =
    {
        .host = "127.0.0.1",
        .ma_port = 3306,
        .db = "testdb",
        .user = "root",
        .password = "p"
};

#endif  // TEST_DB_CREDS_H
