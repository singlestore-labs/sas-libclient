//
// Created by amakarovych-ua on 4/2/21.
//

#ifndef MEMSQL_S2_CLIENT_ERROR_HPP
#define MEMSQL_S2_CLIENT_ERROR_HPP

#include <string>

class S2ClientError : public std::exception
{
  public:
    uint32_t m_errorCode = 0;
    std::string m_errorMessage = "";

    S2ClientError(
        uint32_t errorCode,
        std::string errorMessage)
        :
        m_errorCode(errorCode),
        m_errorMessage(errorMessage)
    {
    }

    const char* what() const noexcept override
    {
        return m_errorMessage.c_str();
    }
};

#define S2C_ERROR_INV_ARG 1            // invalid argument
#define S2C_ERROR_UNS_DATA_TYPE 2      // unsupported data type
#define S2C_ERROR_UNKNOWN_FAILURE 3    // unknown failure
#define S2C_ERROR_MEMORY_ALLOCATION 4  // memory allocation error
#define S2C_ERROR_READER_FAILED 5      // some of the readers failed

#endif  // MEMSQL_S2_CLIENT_ERROR_HPP
