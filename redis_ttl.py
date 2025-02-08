import redis
from redis.exceptions import RedisError
from datetime import datetime
import pymysql
import json
import os

redis_config = {
    'host':os.getenv("REDIS_HOST"),
    'port':os.getenv("REDIS_PORT")
}

config = {
    'host': os.getenv("DB_HOST"),  # 数据库服务器地址
    'user': os.getenv("DB_USER"),       # 数据库用户名
    'password': os.getenv("DB_PASSWORD"),  # 数据库密码
    'database': os.getenv("DB_NAME"),  # 数据库名称
    'charset': 'utf8mb4',  # 字符集
    'cursorclass': pymysql.cursors.DictCursor  # 使用字典形式返回查询结果
}


# 创建数据库连接
def create_connection():
    try:
        connection = pymysql.connect(**config)
        print("数据库连接成功")
        return connection
    except pymysql.MySQLError as e:
        print(f"数据库连接失败: {e}")
        return None

def create_redis_connection(host,port):
    try:
        redis_con= redis.StrictRedis(host=host, port=port, db=0)
        return redis_con
    except Exception as e:
        print(f"redis连接失败: {e}")
        return None

def close_redis_connection(redis_con):
    try:
        redis_con.close()
        print("redis连接已关闭")
        return True
    except RedisError as e:
        print(f"redis连接关闭失败: {e}")
    except Exception as e:
        print(f"未知错误: {e}")
        return False

# 关闭数据库连接
def close_connection(connection):
    if connection:
        connection.close()
        print("数据库连接已关闭")

def get_invaild_event(connection):
    try:
        with connection.cursor() as cursor:
            sql = "select * from ad_linkage where event_id in (select id from ad_events where status in (1)) and event_id <> 1 and event_id <> 5 and event_id <> 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
    except pymysql.MySQLError as e:
        print(f"查询数据失败: {e}")
        return None

def redis_ttl(redis_conn):
    keys = redis_conn.keys('adServer:event*')

    # Iterate over each key
    keys_list = []
    event_list = {}
    for key in keys:
        if "eventMaxPv" not in key.decode("utf-8"):
            continue
        key_arr = key.decode("utf-8").split(":")
        keys_list.append(key_arr)
        d = dict(id = key_arr[2],value=key_arr)
        if key_arr[2] in event_list:
            event_list[key_arr[2]].append(key_arr)
        else:
            event_list[key_arr[2]] = [key_arr]

    for k,v in event_list.items():
        if len(v) == 1:
            rk = ":".join(v[0])
            result = redis_conn.get(rk)
            if result is not None:
                try:
                    result_int = int(result.decode("utf-8"))
                    if result_int == 0:
                        print(result_int)
                        redis_conn.delete(rk)
                except Exception as e:
                    redis_conn.delete(rk)
                    print(f"{rk} error {e}")
    # Close the connections (optional, as redis-py handles connection pooling)

def get_zone_event_info(redis_conn,zone_id,publisher_id,event_id):
    try:
        key = f"adServer:zone:{zone_id}:{publisher_id}"
        result = redis_conn.hget(key,event_id)
        if result is not None:
            result_str = result.decode("utf-8")
            json_result = json.loads(result_str)
            event_key = f"adServer:event:{event_id}:loginfo:eventMaxPv"
            event_max_pv = redis_conn.get(event_key)
            if event_max_pv is None:
                print(f"{event_key} not found")
                return
            event_max_pv_number = int(event_max_pv.decode("utf-8"))
            if event_max_pv_number > json_result["totalMaxPv"]:
                print(f"error :{event_key} {event_max_pv_number} > {json_result['totalMaxPv']}")
                delete_event_key(redis_conn,zone_id,publisher_id,event_id)
            else:
                print(f"info :{event_key} {event_max_pv_number} <= {json_result['totalMaxPv']}")
    except Exception as e:
        print(f"Error: {e}")

def delete_zone_key(redis_conn,zone_id,publisher_id):
    key = f"adServer:zone:{zone_id}:{publisher_id}"
    result = redis_conn.hdelete(key)
    if result is None:
        print(f"delete {key} failed")

def delete_event_key(redis_conn,zone_id,publisher_id,event_id):
    key = f"adServer:event:{event_id}"
    result = redis_conn.delete(key)
    if result is None:
        print(f"delete {key} failed")

def redis_info(db_conn,redis_conn):
    result = get_invaild_event(db_conn) 
    if result is None:
        print("查询数据失败")
        return
    for row in result:
        get_zone_event_info(redis_conn,row["zone_id"],row["publisher_id"],row["event_id"])



if __name__ == "__main__":
    db_conn = create_connection()
    redis_conn = create_redis_connection(**redis_config)
    if db_conn is None and redis_conn is None:
        exit(1)
    result = redis_info(db_conn,redis_conn)
    close_connection(db_conn)
    close_redis_connection(redis_conn)
