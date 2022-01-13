from dbutils.pooled_db import PooledDB
import pymysql
import pymysql.cursors


config = {
    "host": 'localhost',
    "user": 'root',
    "password": '123456',
    "database":'fed_sql',
    "port": 3306
}


class BaseDB:
    def __init__(self, config):
        super().__init__()

        self.pool = PooledDB(pymysql, 10, **config)

    def create_connection(self):
        connection = self.pool.connection()
        cursor = connection.cursor()
        return connection, cursor

    def close_connection(self, connection, cursor):
        connection.close()
        cursor.close()

    def select_one(self, sql, args):
        conn, cur = self.create_connection()
        cur.execute(sql, args)
        result = cur.fetchone()
        self.close_connection(conn, cur)
        return result

    def select_all(self, sql, args=[]):
        conn, cur = self.create_connection()
        cur.execute(sql, args)
        result = cur.fetchall()
        self.close_connection(conn, cur)
        return result

    def revise_one(self, sql, args):
        conn, cur = self.create_connection()
        result = cur.execute(sql, args)
        conn.commit()
        self.close_connection(conn, cur)
        return result


calculate_func = {
    "-": lambda a, b: a - b,
    "+": lambda a, b: a + b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b
}

compare_func = {
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "=": lambda a, b: a == b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "!=": lambda a, b: a != b,

}
conj_func = {
    "AND": lambda a, b: a & b,
    "OR": lambda a, b: a | b
}

dbop = BaseDB(config)