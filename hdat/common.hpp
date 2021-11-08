#ifndef HDAT_COMMON_HPP
#define HDAT_COMMON_HPP

#include <sstream>

#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <mysql.h>

#include <chunk_extern.h>
#include <s2_client_error.hpp>

const int defaultAlignmentSize = 8;

const int64_t secToMicroSec = 1000000;
const int64_t dayToSec = 86400;             // 24 * 60 * 60
const int64_t dayToMicroSec = 86400000000;  // 24 * 60 * 60 * 1000000

static const char dateFormat[] = "%Y-%m-%d";               // datetime-based formatting
static const char dateTimeFormat[] = "%Y-%m-%d %H:%M:%S";  // datetime-based formatting
static const char dateTimeMicroSec[] = ".%d";              // integer-based formatting

inline const time_t zeroTimeCAS()
{
    struct tm zeroDate =
        {
            .tm_sec = 0,
            .tm_min = 0,
            .tm_hour = 0,
            .tm_mday = 1,
            .tm_mon = 0,
            .tm_year = 60,
        };
    return timegm(&zeroDate);
}

inline const time_t zeroTimeUnix()
{
    struct tm zeroDate =
        {
            .tm_sec = 0,
            .tm_min = 0,
            .tm_hour = 0,
            .tm_mday = 1,
            .tm_mon = 0,
            .tm_year = 70,
        };
    return timegm(&zeroDate);
}

const time_t zeroTimePointCAS = zeroTimeCAS();
const time_t zeroTimePointUnix = zeroTimeUnix();
const int64_t CASshiftSec = (int64_t)difftime(zeroTimePointUnix, zeroTimePointCAS);

// toTime returns the number of microsenconds since midnight
inline int64_t toTimeCAS(const char* input)
{
    const MYSQL_TIME* dt = (MYSQL_TIME*)input;
    return dt->second_part + secToMicroSec * (dt->second + dt->minute * 60 + dt->hour * 3600);
}

inline std::string fromTimeCAS(const int64_t cas_time)
{
    int64_t total_seconds = cas_time / secToMicroSec;
    int32_t seconds = total_seconds % 60;
    int32_t total_minutes = (int32_t)total_seconds / 60;
    int32_t minutes = total_minutes % 60;
    int32_t hours = total_minutes / 60;
    std::stringstream ss;
    ss << hours << ":" << minutes << ":" << seconds;
    return ss.str();
}

// toDate returns the number of days since 1 Jan 1960
inline int32_t toDateCAS(const char* input)
{
    const MYSQL_TIME* dt = (MYSQL_TIME*)input;

    struct tm c_datetime;

    c_datetime.tm_year = dt->year - 1900;
    c_datetime.tm_mon = dt->month - 1;
    c_datetime.tm_mday = dt->day;

    // we don't copy time information since here
    // we are only interested in the date
    c_datetime.tm_hour = 0;
    c_datetime.tm_min = 0;
    c_datetime.tm_sec = 0;

    time_t inputDate = timegm(&c_datetime);
    if (inputDate == time_t(-1))
    {
        throw S2ClientError(S2C_ERROR_INV_ARG, "Invalid Date read from DB");
    }

    // difftime returns seconds
    double days = difftime(inputDate, zeroTimePointCAS) / (3600 * 24);

    return (int32_t)days;
}

inline std::string fromDateCAS(const int32_t dtInt)
{
    time_t UnixTimestamp = dtInt * dayToSec - CASshiftSec;

    struct tm* c_datetime = gmtime(&UnixTimestamp);

    char c_string_result[11];
    strftime(c_string_result, 11, dateFormat, c_datetime);
    return std::string(c_string_result);
}

// toDateTime returns number of microseconds since midnight 1 Jan 1960
inline int64_t toDateTimeCAS(const char* input)
{
    const MYSQL_TIME* dt = (MYSQL_TIME*)input;

    struct tm c_datetime;

    c_datetime.tm_year = dt->year - 1900;
    c_datetime.tm_mon = dt->month - 1;
    c_datetime.tm_mday = dt->day;

    c_datetime.tm_hour = dt->hour;
    c_datetime.tm_min = dt->minute;
    c_datetime.tm_sec = dt->second;

    time_t inputDate = timegm(&c_datetime);
    if (inputDate == time_t(-1))
    {
        throw S2ClientError(S2C_ERROR_INV_ARG, "Invalid Date read from DB");
    }
    return (ino64_t)difftime(inputDate, zeroTimePointCAS) * secToMicroSec + dt->second_part;
}

inline std::string fromDateTimeCAS(const int64_t dtInt)
{
    int32_t microSec = dtInt % secToMicroSec;
    time_t UnixTimestamp = dtInt / secToMicroSec - CASshiftSec;

    struct tm* c_datetime = gmtime(&UnixTimestamp);
    char c_string_result[30];
    int n_written = strftime(c_string_result, 30, dateTimeFormat, c_datetime);
    if (microSec)
    {
        sprintf(c_string_result + n_written, dateTimeMicroSec, microSec);
    }
    return std::string(c_string_result);
}

inline uint64_t sizeofAligned8(uint64_t len)
{
    return (len + defaultAlignmentSize - 1) & ~(defaultAlignmentSize - 1);
}

inline uint64_t
rowSize(
    const RowSchema* schema,
    const unsigned long* lengths)
{
    uint64_t total_size = 0;
    for (auto i = 0; i < schema->numColumns; ++i)
    {
        switch (schema->ColumnInfo[i].type)
        {
            case Variable:
            {
                total_size += lengths[i] + 2 * defaultAlignmentSize;
                break;
            }
            case Fixed:
            {
                total_size += sizeofAligned8(schema->ColumnInfo[i].size);
                break;
            }
            default:
                total_size += defaultAlignmentSize;
        }
    }
    return total_size;
}

#endif  // HDAT_COMMON_HPP
