import os
from pypinyin import lazy_pinyin
import pandas as pd
import pymysql


def read_filename(filename):
    filename = filename.lstrip("全国自然灾害综合风险清查-")
    filename = filename.rstrip("(1).xlsx")
    pinyin = lazy_pinyin(filename)
    capitals = "".join([p[0] for p in pinyin if len(p) > 0 and not p.__contains__("（") and not p.__contains__("）") and not p.__contains__("、")])
    return capitals, filename


def read_columns(filename):
    df = pd.read_excel("tables/" + filename)
    columns = df.columns.values
    column_map = {}
    i = 0
    for c in columns:
        column_map["column" + str(i)] = c
        i += 1
    return column_map


def generate_ddl(filename):
    table_name, table_comment = read_filename(filename)
    column_map = read_columns(filename)
    column_definition_format = "`{}` varchar(255) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '{}'"
    column_definition = ",\n".join(column_definition_format.format(name, comment)
                                   for (name, comment) in column_map.items())
    create_statement = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
                                {column_definition})
                        DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='{table_comment}';\n"""
    return create_statement


def write_ddl():
    with open("govern_ddl.sql", "a+") as f:
        for filename in os.listdir("./tables"):
            ddl = generate_ddl(filename)
            f.write(ddl)


def read_data(filename):
    df = pd.read_excel("tables/" + filename)
    row_index = df.index.values
    return df.loc[row_index[0:]].values


def connect_to_database():
    connection = pymysql.connect(host="111111111", port=3306, user="11111111", passwd="111111111", database="111111111")
    return connection


def insert_data(filename):
    data_list = read_data(filename)
    table_name, _ = read_filename(filename)
    connection = connect_to_database()
    with connection.cursor() as cursor:
        for data in data_list:
            print(data)
            values = []
            for v in data:
                if pd.notna(v):
                    v = str(v).replace("'", "")
                    values.append("'"+str(v)+"'")
                else:
                    values.append('NULL')
            sql = f"insert into {table_name} values ({','.join(values)});"
            print(sql)
            cursor.execute(sql)
    connection.commit()
    connection.close()


def bulk_insert_data():
    for filename in os.listdir("./tables"):
        insert_data(filename)


if __name__ == '__main__':
    # print(read_filename("全国自然灾害综合风险清查-(1).xlsx"))
    # print(read_columns("全国自然灾害综合风险清查-(1).xlsx"))
    # print(generate_ddl("地质灾害隐患基本情况.xls"))
    # print(read_data("全国自然灾害综合风险清查(1).xlsx"))
    # write_ddl()
    # connect_to_database()
    insert_data("地质灾害隐患基本情况.xls")
    # bulk_insert_data()
