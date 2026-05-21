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
