# !/bin/env python
import json
import pymysql
import re

pattern = re.compile(r'GET\s[^\?]+\?([^\s]+)\sHTTP')

config = {
    'host': 'adserverdb.c16aoo6syncd.us-west-1.rds.amazonaws.com',  # 数据库服务器地址
    'user': 'adtest',       # 数据库用户名
    'password': 'XA5BTb1iTbm5A3Q4M3yi',  # 数据库密码
    'database': 'adserver',  # 数据库名称
    'charset': 'utf8mb4',  # 字符集
    'cursorclass': pymysql.cursors.DictCursor  # 使用字典形式返回查询结果
}

conn = None

# 创建数据库连接
def create_connection():
    try:
        connection = pymysql.connect(**config)
        print("数据库连接成功")
        return connection
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        return None

# 关闭数据库连接
def close_connection(connection):
    if connection:
        connection.close()
        print("数据库连接已关闭")

# 插入数据
def insert_data(connection,sql,params):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql,params)
            connection.commit()
            print("数据插入成功")
    except pymysql.MySQLError as e:
        print(f"插入数据失败: {e}")

def test_mysql(conn):
    try:
        with conn.cursor() as cursor:
            sql = "show tables"
            cursor.execute(sql)
            result = cursor.fetchall()
            for row in result:
                print(row)
    except pymysql.MySQLError as e:
        print(f"test mysql error:{e}")

def camel_to_snake(name):
    """
    将驼峰命名法转换为蛇形命名法
    :param name: 驼峰命名的字符串
    :return: 蛇形命名的字符串
    """
    # 使用正则表达式匹配大写字母，并在前面加上下划线，然后将整个字符串转换为小写
    snake_case = re.sub(r'([A-Z])', r'_\1', name).lower()
    return snake_case.lstrip('_')  # 去掉开头的下划线（如果有）

def parse_log(log):
    match = pattern.search(log)
    if match:
        query_string = match.group(1)
        kv_pairs = query_string.split('&')
        kv_dict = {}
        for pair in kv_pairs:
            key, value = pair.split('=')
            if key == "ajaxTime" or key == 'rt' or key == 'sid' or key == '_t' or key == 'channel':
                continue
            snake_key = camel_to_snake(key)
            kv_dict[snake_key] = value
        if kv_dict.get("request_type") is None:
            kv_dict["request_type"] = "getAd"
        if kv_dict.get("event_id") is None:
            kv_dict["event_id"] = 0
        return kv_dict
    else:
        print("No match found")
        return None

def json2sql(table_name,data_dict):
    """
    将字典转换为 MySQL 的 INSERT 语句
    :param table_name: 表名
    :param data_dict: 包含字段名和值的字典
    :return: SQL 语句和参数列表
    """
    if not isinstance(data_dict, dict) or not data_dict:
        raise ValueError("data_dict 必须是一个非空字典")

    # 提取字段名和值
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['%s'] * len(data_dict))  # 使用 %s 作为占位符
    values = list(data_dict.values())

    # 构造 SQL 语句
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    print(sql)
    return sql, values

def json2sql_str(table_name,data_dict):
    """
    将字典转换为 MySQL 的 INSERT 语句
    :param table_name: 表名
    :param data_dict: 包含字段名和值的字典
    :return: SQL 语句
    """
    if not isinstance(data_dict, dict) or not data_dict:
        raise ValueError("data_dict 必须是一个非空字典")

    # 提取字段名和值
    columns = ', '.join(data_dict.keys())
    # placeholders = ', '.join(['%s'] * len(data_dict))  # 使用 %s 作为占位符
    placeholders = ', '.join([f"'{value}'" for value in data_dict.values()])
    # values = list(data_dict.values())

    # 构造 SQL 语句
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    return sql


def get_log(conn,log_file):
    with open(log_file, 'r') as file:
        logs = file.readlines()
        for log in logs:
            kv = parse_log(log)
            if kv is not None:
                sql_str,values=json2sql('data_raw_log_20250204',kv)
                insert_data(conn,sql_str,values)
                

if __name__ == '__main__':
    conn = create_connection()
    logfile = ['20250204.txt']
    for log in logfile:
        get_log(conn,log)
    
    close_connection(conn)