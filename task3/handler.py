import mysql.connector as pymysql
import numpy as np
import pandas as pd

def create_happyness_table_and_insert_data(csv_file_path, db_config):
    """
    创建幸福指数数据表并从 CSV 文件插入数据。

    参数:
        db_config (dict): 数据库配置。
        csv_file_path (str): 幸福指数表的 CSV 文件路径。
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

        # 创建表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS happyness (
            Overall_rank INT,
            Country VARCHAR(255),
            Score FLOAT,
            GDP_per_capita FLOAT,
            Social_support FLOAT,
            Healthy_life_expectancy FLOAT,
            Freedom_to_make_life_choices FLOAT,
            Generosity FLOAT,
            Perceptions_of_corruption FLOAT
        );
        """
        cursor.execute(create_table_query)

        # 插入数据
        for _, row in data.iterrows():
            insert_query = """
            INSERT INTO happyness (Overall_rank, Country, Score, GDP_per_capita, Social_support, Healthy_life_expectancy, Freedom_to_make_life_choices, Generosity, Perceptions_of_corruption)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE Score=VALUES(Score);
            """
            cursor.execute(insert_query, (
                int(row["Overall rank"]),
                row["Country or region"],
                float(row["Score"]),
                float(row["GDP per capita"]),
                float(row["Social support"]),
                float(row["Healthy life expectancy"]),
                float(row["Freedom to make life choices"]),
                float(row["Generosity"]),
                float(row["Perceptions of corruption"])
            ))
        connection.commit()
        print("幸福指数数据表创建并插入数据成功！")
    except Exception as e:
        print(f"创建表或插入数据失败: {e}")
    finally:
        connection.close()

def fetch_regression_data(db_config):
    """
    从 MySQL 数据库中提取回归分析所需的数据。
    """
    try:
        # 连接 MySQL 数据库
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # 提取数据
        query = """
        SELECT GDP_per_capita, Social_support, Healthy_life_expectancy, Freedom_to_make_life_choices, Generosity, Perceptions_of_corruption, Score
        FROM happyness;
        """
        cursor.execute(query)
        data = cursor.fetchall()
        return np.array(data)
    except Exception as e:
        print(f"提取数据失败: {e}")
        return None
    finally:
        connection.close()

def perform_regression(data):
    """
    使用 numpy 进行多元线性回归分析。

    参数:
        data (np.array): 包含自变量和因变量的数据。

    返回:
        np.array: 回归系数。
    """
    # 提取自变量 (X) 和因变量 (Y)
    X = data[:, :-1]  # 自变量
    Y = data[:, -1]   # 因变量

    # 添加常数项 (截距)
    X = np.hstack([np.ones((X.shape[0], 1)), X])

    # 计算回归系数
    beta = np.linalg.inv(X.T @ X) @ X.T @ Y
    return beta