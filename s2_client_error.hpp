#ifndef S2_CLIENT_ERROR_HPP
#define S2_CLIENT_ERROR_HPP

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

    S2ClientError()
    {
        S2ClientError(0, "");
    }

    const char* what() const noexcept override
    {
        return m_errorMessage.c_str();
    }
};

#endif  // MEMSQL_S2_CLIENT_ERROR_HPP
