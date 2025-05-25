import mysql.connector
import task1.handler as handler1
import os 
import task2.handler as handler2
import task3.handler as handler3 

db_config = {
    "username": "root", 
    "password": "Lzx25226",
    "host":     "localhost", 
    "database": "DBPractice05", 
    "charset" : "utf8mb4"
}

def create_database():
    """新建数据库 DBPractice05"""
    conn = None
    cursor = None
    try:
        # 连接到 MySQL
        conn = mysql.connector.connect(
            username="root",
            password="Lzx25226",
            host="localhost",
        )
        cursor = conn.cursor()

        # 创建数据库 DBPractice05
        cursor.execute("CREATE DATABASE IF NOT EXISTS DBPractice05")
        print("数据库 'DBPractice05' 已创建或已存在。")
    except mysql.connector.Error as err:
        print(f"创建数据库时发生错误: {err}")
    finally:
        # 确保关闭游标和数据库连接
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            
def drop_all_tables():
    """删除 DBPractice05 数据库中所有的表"""
    conn = None
    cursor = None
    try:
        # 连接到 DBPractice05 数据库
        conn = mysql.connector.connect(
            username="root",
            password="Lzx25226",
            host="localhost",
            database="DBPractice05"
        )
        cursor = conn.cursor()

        # 查询所有表名
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        # 删除所有表
        for (table_name,) in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"表 '{table_name}' 已删除。")

        conn.commit()
        print("所有表已删除。")
    except mysql.connector.Error as err:
        print(f"删除表时发生错误: {err}")
    finally:
        # 确保关闭游标和数据库连接
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    print("[main] create_database()")
    create_database()
    print("[main] drop all tables")
    drop_all_tables()
    
    show_task_1 = True
    show_task_2 = True
    show_task_3 = True 
    
    # task 1
    if show_task_1: 
        print()
        print("         ### TASK 1 ###")
        print()
        csv_files_dir1 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task1")
        # 加载 CSV 文件到 MySQL
        handler1.load_movies_to_mysql(os.path.join(csv_files_dir1, "movies.csv"), "movies", db_config)
        handler1.load_ratings_to_mysql(os.path.join(csv_files_dir1, "ratings.csv"), "ratings", db_config)
        # 执行查询
        handler1.execute_queries(db_config)
        
    # task 2
    if show_task_2: 
        print() 
        print("         ### TASK 2 ###") 
        print() 
        csv_files_dir2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task2")
        handler2.load_stock_data_to_mysql(os.path.join(csv_files_dir2, "Google.csv"), "google_stock", db_config)
        handler2.calculate_golden_cross_mysql("google_stock", db_config)

        # 加载 Apple 的股票数据
        handler2.load_stock_data_to_mysql(os.path.join(csv_files_dir2, "Apple.csv"), "apple_stock", db_config)
        handler2.calculate_golden_cross_mysql("apple_stock", db_config)

        # 加载 Tesla 的股票数据
        handler2.load_stock_data_to_mysql(os.path.join(csv_files_dir2, "Tesla.csv"), "tesla_stock", db_config)
        handler2.calculate_golden_cross_mysql("tesla_stock", db_config)
        
    # task 3
    if show_task_3: 
        print() 
        print("         ### Task 3 ###") 
        print() 
        csv_file_dir3 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task3", "world-happiness")
        handler3.create_happyness_table_and_insert_data(os.path.join(csv_file_dir3, "2019.csv"), db_config)
        data = handler3.fetch_regression_data(db_config)
        if data is not None:
            beta = handler3.perform_regression(data)
            print("回归系数:", beta)    