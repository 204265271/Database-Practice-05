import pandas as pd
import mysql.connector as pymysql

def load_stock_data_to_mysql(csv_file_path, table_name, db_config):
    """
    从 CSV 文件读取股票数据并插入到 MySQL 数据库的表中。

    参数:
        csv_file_path (str): CSV 文件路径。
        table_name (str): 数据库表名。
        db_config (dict): 数据库配置。
    """
    try:
        # 读取 CSV 文件
        data = pd.read_csv(csv_file_path)
        print(f"CSV 文件 {csv_file_path} 读取成功！")
    except Exception as e:
        print(f"读取 CSV 文件失败: {e}")
        return

    try:
        # 连接 MySQL 数据库
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # 如果表不存在，则创建表
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date DATE NOT NULL,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Volume BIGINT,
            PRIMARY KEY (Date)
        );
        """
        cursor.execute(create_table_query)
        print(f"表 {table_name} 创建成功或已存在！")

        # 插入数据
        for _, row in data.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (Date, Open, High, Low, Close, Volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
        connection.commit()
        print(f"数据成功插入到表 {table_name} 中！")
    except Exception as e:
        print(f"插入数据失败: {e}")
    finally:
        connection.close()


def calculate_golden_cross_mysql(table_name, db_config):
    """
    使用 MySQL 查询计算金叉（5 日线上穿 10 日线）出现的位置。

    参数:
        table_name (str): 数据库表名。
        db_config (dict): 数据库配置。
    """
    try:
        # 连接 MySQL 数据库
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        query = f"""
        SELECT
            t1.Date,
            t1.Close
        FROM (
            SELECT
                Date,
                Close,
                AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA5,
                AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS MA10
            FROM {table_name}
        ) t1
        JOIN (
            SELECT
                Date,
                Close,
                AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS MA5,
                AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS MA10
            FROM {table_name}
        ) t2
        ON t1.Date = DATE_ADD(t2.Date, INTERVAL 1 DAY)
        WHERE t1.MA5 > t1.MA10 AND t2.MA5 <= t2.MA10;
        """
        cursor.execute(query)

        # 输出结果
        print(f"\n金叉出现位置 - {table_name}:")
        for row in cursor.fetchall():
            print(row)

    except Exception as e:
        print(f"查询金叉失败: {e}")
    finally:
        connection.close()