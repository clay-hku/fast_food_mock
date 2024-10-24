#coding=utf-8
import json
import os
import configparser
import pymysql
import numpy as np
import getopt
import sys

def load_properties():
    config = configparser.ConfigParser()
    config.read('ads_to_mysql_datax.properties', encoding='utf-8')
    global mysql_host, mysql_port, mysql_user, mysql_password, hdfs_host, hdfs_port, export_path, table_schema, table_names
    table_names = config.get('mysql', 'table_names').split(', ')
    table_schema = config.get('mysql', 'table_schema')
    mysql_host = config.get('mysql', 'host')
    mysql_port = config.get('mysql', 'port')
    mysql_user = config.get('mysql', 'user')
    mysql_password = config.get('mysql', 'password')
    hdfs_host = config.get('hdfs', 'nn_host')
    hdfs_port = config.get('hdfs', 'nn_port')
    export_path = config.get('export', 'path')

def get_connection():
    connection = pymysql.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, password=mysql_password)
    return connection



def get_table_names(table_schema, table):
    connection = get_connection()
    cursor = connection.cursor()
    sql = """
            select 
                    t.COLUMN_NAME,
                    t.DATA_TYPE
            from information_schema.COLUMNS t
            where t.TABLE_SCHEMA = %s and t.TABLE_NAME = %s
            order by t.ORDINAL_POSITION;
    """
    cursor.execute(sql, [table_schema, table])
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    result = list(map(lambda x: x[0], result))
    res = list(map(lambda x: {'name':x[0], 'type':x[1]}, result))
    return result, res

# 生成json文件
def generate_json(table_schema, table):
    res, col = get_table_names(table_schema, table)
    json_data = \
        {
            "job": {
                "setting": {
                    "speed": {
                        "channel": 3
                    }
                },
                "content": [
                    {
                        "reader": {
                            "name": "hdfsreader",
                            "parameter": {
                                "path": "${exportdir}",
                                "defaultFS": "hdfs://hadoop102:8020",
                                "column": ["*"],
                                "fileType": "text",
                                "encoding": "UTF-8",
                                "fieldDelimiter": "\t",
                                "nullFormat": "\\N"
                            }

                        },
                        "writer": {
                            "name": "mysqlwriter",
                            "parameter": {
                                "writeMode": "insert",
                                "username": "root",
                                "password": "123456",
                                "column": res,
                                "connection": [
                                    {
                                        "jdbcUrl":  "jdbc:mysql://" + mysql_host + ":" + mysql_port + "/" + table_schema + "?useUnicode=true&characterEncoding=utf-8",
                                        "table": [
                                            table
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        }

    if os.path.exists(export_path):
        with open(export_path + '/' + '.'.join([table_schema,table,'json']), 'w') as f:
            json.dump(json_data, f)
    else:
        os.makedirs(export_path)
        with open(export_path + '/' + '.'.join([table_schema,table,'json']), 'w') as f:
            json.dump(json_data, f)
    return json_data
def main(args):
    load_properties()
    schema, table = '', ''
    options, arguments = getopt.getopt(args, '-s:-t:', ['schema=', 'table='])
    for opt_name, opt_value in options:
        if opt_name in ('-s', '--schema'):
            schema = opt_value
        if opt_name in ('-t', '--table'):
            table = opt_value
    if schema and table:
        print('generating json file for table: ' + schema + '.' + table)
        generate_json(schema, table)
        print('json file generated successfully')
    else:
        for i, table in enumerate(table_names):
            print(i+1, '. generating json file for table: ' + table_schema + '.' + table, sep='')
            generate_json(table_schema, table)
            print(i+1,'. json file generated successfully ', sep='')
    return None




if __name__ == '__main__':
    main(sys.argv[1:])
