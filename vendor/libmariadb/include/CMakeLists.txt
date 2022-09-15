set(MARIADB_CLIENT_INCLUDES
    ${CC_SOURCE_DIR}/include/mariadb_com.h
    ${CC_SOURCE_DIR}/include/mysql.h
    ${CC_SOURCE_DIR}/include/mariadb_stmt.h
    ${CC_SOURCE_DIR}/include/ma_pvio.h
    ${CC_SOURCE_DIR}/include/ma_tls.h
    ${CC_BINARY_DIR}/include/mariadb_version.h
    ${CC_SOURCE_DIR}/include/ma_list.h
    ${CC_SOURCE_DIR}/include/errmsg.h
    ${CC_SOURCE_DIR}/include/mariadb_dyncol.h
    ${CC_SOURCE_DIR}/include/mariadb_ctype.h
    ${CC_SOURCE_DIR}/include/mariadb_rpl.h)
if(NOT IS_SUBPROJECT)
  set(MARIADB_CLIENT_INCLUDES ${MARIADB_CLIENT_INCLUDES}
                              ${CC_SOURCE_DIR}/include/mysqld_error.h)
endif()
set(MYSQL_ADDITIONAL_INCLUDES
    ${CC_SOURCE_DIR}/include/mysql/client_plugin.h
    ${CC_SOURCE_DIR}/include/mysql/plugin_auth_common.h
    ${CC_SOURCE_DIR}/include/mysql/plugin_auth.h)
set(MARIADB_ADDITIONAL_INCLUDES ${CC_SOURCE_DIR}/include/mariadb/ma_io.h)
if(WIN32)
  set(WIX_INCLUDES
      ${MARIADB_CLIENT_INCLUDES} ${MARIADB_ADDITIONAL_INCLUDES}
      ${MYSQL_ADDITIONAL_INCLUDES}
      PARENT_SCOPE)
endif()

install(
  FILES ${MARIADB_CLIENT_INCLUDES}
  DESTINATION ${INSTALL_INCLUDEDIR}
  COMPONENT Development)
install(
  FILES ${MYSQL_ADDITIONAL_INCLUDES}
  DESTINATION ${INSTALL_INCLUDEDIR}/mysql
  COMPONENT Development)
install(
  FILES ${MARIADB_ADDITIONAL_INCLUDES}
  DESTINATION ${INSTALL_INCLUDEDIR}/mariadb
  COMPONENT Development)
