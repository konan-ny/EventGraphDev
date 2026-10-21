from __future__ import annotations

import psycopg
import pymysql
import pymysql.cursors

from .config import Settings


def pg_connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn)


def mysql_connect(settings: Settings) -> pymysql.Connection:
    return pymysql.connect(
        host=settings.db_host,
        user=settings.db_user,
        password=settings.db_pwd,
        database=settings.db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
