import csv
import random
from abc import ABC
from typing import Any
from datetime import datetime, timedelta
from db_conns import get_pg_conn
from psycopg2 import sql, extras

# папка public доступна всем пользователям. Если создавать файл не тут, то возникает ошибка доступа
TMP_DATAFILE_NAME = "C:/Users/Public/datafile.csv"


class GenDataType(ABC):
    @classmethod
    def gen_random_value(cls) -> Any:
        pass


class GenIntType(GenDataType):
    # согласно размерности типа int
    MIN_INT_BORDER = -2147483648
    MAX_INT_BORDER = 2147483647

    @classmethod
    def gen_random_value(cls) -> int:
        return random.randint(cls.MIN_INT_BORDER, cls.MAX_INT_BORDER)


class GenStrType(GenDataType):

    # согласно ASCII таблице
    # можно использовать библиотеку string
    MIN_INT_BORDER = 48
    MAX_INT_BORDER = 126
    STR_LEN = 35

    @classmethod
    def gen_random_value(cls) -> str:
        rand_str = ""
        for _ in range(cls.STR_LEN):
            rand_str += chr(random.randint(cls.MIN_INT_BORDER, cls.MAX_INT_BORDER))
        return rand_str


class GenTimestampType(GenDataType):

    START_TIME = datetime(1800, 1, 1)
    END_TIME = datetime(9999, 12, 31)

    @classmethod
    def gen_random_value(cls) -> datetime:
        time_delta = cls.END_TIME - cls.START_TIME
        total_seconds = int(time_delta.total_seconds())
        random_seconds = random.randrange(total_seconds)
        return cls.START_TIME + timedelta(seconds=random_seconds)


# TODO use registy pattern
type_cls_mapping: dict[str, type[GenDataType]] = {
    "int": GenIntType,
    "integer": GenIntType,
    "bigint": GenIntType,
    "str": GenStrType,
    "string": GenStrType,
    "varchar": GenStrType,
    "character varying": GenStrType,
    "text": GenStrType,
    "timestamp": GenTimestampType,
    "timestamp without time zone": GenTimestampType,
}


def gen_data(number_of_rows: int, attrs_type_arr: list[str]) -> list[list]:
    data = []
    for i in range(number_of_rows):
        data_row = []
        for attr_type in attrs_type_arr:

            gen_cls = type_cls_mapping[attr_type]
            data_row.append(gen_cls.gen_random_value())
        data.append(data_row)
    print("First 5 generated rows:")
    for i in range(5):
        print(f"Строка {i+1}: {data[i]}")
    return data


def write_csv(data: list[list]):
    with open(TMP_DATAFILE_NAME, "w", encoding="utf8", newline="") as csv_file:
        writer = csv.writer(csv_file, delimiter=",")
        for line in data:
            print(line)
            writer.writerow(line)


def fill_pg_table_with_random_data(
    schema_name: str, table_name: str, truncate_flg: bool
):
    conn = get_pg_conn(
        db_name="postgres",
        user_name="postgres",
        password="",
        host_name="localhost",
        port="5432",
    )
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    get_table_cols_info_query = sql.SQL(
        """
select column_name, data_type
from information_schema.columns
where table_schema = {schema_name}
	and table_name = {table_name}
"""
    ).format(schema_name=sql.Literal(schema_name), table_name=sql.Literal(table_name))
    cur.execute(get_table_cols_info_query)
    res = cur.fetchall()
    # print(res)

    attrs_type_arr: list[str] = []
    column_name_arr: list[str] = []
    for row in res:
        attrs_type_arr.append(row["data_type"])
        column_name_arr.append(row["column_name"])
    # print(attrs_type_arr)
    # print(column_name_arr)

    gen_data(number_of_rows=10, attrs_type_arr=attrs_type_arr)

    if truncate_flg:
        truncate_query = sql.SQL(
            "truncate table {schema_name}.{table_name} restart identity cascade;"
        ).format(
            schema_name=sql.Identifier(schema_name),
            table_name=sql.Identifier(table_name),
        )
        # print(truncate_query)
        if cur.query:
            print(cur.query.decode("utf-8"))
        cur.execute(truncate_query)

    load_data_to_table_query = sql.SQL(
        """
copy {schema_name}.{table_name}({column_list})
from {TMP_DATAFILE_NAME} delimiter ',' csv NULL 'null';
"""
    ).format(
        schema_name=sql.Identifier(schema_name),
        table_name=sql.Identifier(table_name),
        column_list = sql.SQL(', ').join(sql.Identifier(n) for n in column_name_arr),
        TMP_DATAFILE_NAME=sql.Literal(TMP_DATAFILE_NAME),
    )
    cur.execute(load_data_to_table_query)
    if cur.query:
        print(cur.query.decode("utf-8"))
    cur.close()
    conn.commit()
    conn.close()
