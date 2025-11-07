from enum import Enum

class Driver(Enum):
    MYSQL = "com.mysql.cj.jdbc.Driver"
    POSTGRESQL = "org.postgresql.Driver"
    SQL_SERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    ORACLE = "oracle.jdbc.driver.OracleDriver"