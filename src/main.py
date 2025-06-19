from generator import fill_pg_table_with_random_data


def main():
    fill_pg_table_with_random_data(schema_name="a_b_layer", table_name="test", truncate_flg=False)


if __name__ == "__main__":
    main()
