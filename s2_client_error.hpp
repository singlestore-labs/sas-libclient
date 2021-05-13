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

#endif  // MEMSQL_S2_CLIENT_ERROR_HPP
