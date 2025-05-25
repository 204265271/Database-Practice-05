import pandas as pd
import mysql.connector as pymysql 

def execute_queries(db_config):
    """
    执行四个查询任务。
    """
    try:
        # 连接 MySQL 数据库
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        test = False
        if test == True:
            # 0. test only 
            print("\n0. test only for movies") 
            cursor.execute("""
                SELECT * 
                FROM movies
                LIMIT 10
            """)
            for row in cursor.fetchall(): 
                print(row)
                
            # 0.1 test only 
            print("\n0.1 test only for ratings") 
            cursor.execute("""
                select * 
                from ratings
                limit 10
            """)
            for row in cursor.fetchall(): 
                print(row)

        # 1. 列出平均得分前10的电影
        print("\n1. 平均得分前10的电影: ")
        cursor.execute("""
            SELECT movies.movieId, movies.title, AVG(ratings.rating) AS avg_rating
            FROM movies
            JOIN ratings ON movies.movieId = ratings.movieId
            GROUP BY movies.movieId, movies.title
            ORDER BY avg_rating DESC
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(row)

        # 2. 列出每个类型的平均得分前10的电影
        print("\n2. 每个类型的平均得分前10的电影: ")
        cursor.execute("""
            SELECT genres.genre, movies.movieId, movies.title, AVG(ratings.rating) AS avg_rating
            FROM movies
            JOIN ratings ON movies.movieId = ratings.movieId
            JOIN (
                SELECT movieId, genre
                FROM (
                    SELECT movieId, SUBSTRING_INDEX(SUBSTRING_INDEX(genres, '|', n), '|', -1) AS genre
                    FROM movies
                    JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) numbers
                ) genres_expanded
            ) genres ON movies.movieId = genres.movieId
            GROUP BY genres.genre, movies.movieId, movies.title
            ORDER BY genres.genre, avg_rating DESC
        """)
        current_genre = None
        count = 0
        for row in cursor.fetchall():
            genre = row[0]
            if genre != current_genre:
                current_genre = genre
                count = 0
                print(f"\nGenre: {genre}")
            if count < 10:
                print(row)
                count += 1
                
        # 3. 列出每个用户综合评价排在前5的电影类型
        print("\n3. 每个用户综合评价排在前5的电影类型: ")
        cursor.execute("""
            SELECT ratings.userId, genres.genre, AVG(ratings.rating) AS avg_rating
            FROM ratings
            JOIN (
                SELECT movieId, genre
                FROM (
                    SELECT movieId, SUBSTRING_INDEX(SUBSTRING_INDEX(genres, '|', n), '|', -1) AS genre
                    FROM movies
                    JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) numbers
                ) genres_expanded
            ) genres ON ratings.movieId = genres.movieId
            GROUP BY ratings.userId, genres.genre
            ORDER BY ratings.userId, avg_rating DESC
        """)
        current_user = None
        count = 0
        for row in cursor.fetchall():
            user_id = row[0]
            if user_id != current_user:
                current_user = user_id
                count = 0
                print(f"\nUser ID: {user_id}")
            if count < 5:
                print(row)
                count += 1

        # 4. 列出每个用户观影次数排在前5的电影类型
        print("\n4. 每个用户观影次数排在前5的电影类型: ")
        cursor.execute("""
            SELECT ratings.userId, genres.genre, COUNT(*) AS watch_count
            FROM ratings
            JOIN (
                SELECT movieId, genre
                FROM (
                    SELECT movieId, SUBSTRING_INDEX(SUBSTRING_INDEX(genres, '|', n), '|', -1) AS genre
                    FROM movies
                    JOIN (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) numbers
                ) genres_expanded
            ) genres ON ratings.movieId = genres.movieId
            GROUP BY ratings.userId, genres.genre
            ORDER BY ratings.userId, watch_count DESC
        """)
        current_user = None
        count = 0
        for row in cursor.fetchall():
            user_id = row[0]
            if user_id != current_user:
                current_user = user_id
                count = 0
                print(f"\nUser ID: {user_id}")
            if count < 5:
                print(row)
                count += 1

    except Exception as e:
        print(f"查询时发生错误: {e}")
    finally:
        connection.close()

def load_movies_to_mysql(csv_file_path, table_name, db_config):
    """
    从 movies.csv 文件读取数据并插入到 MySQL 数据库的 movies 表中。
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
            movieId INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            genres VARCHAR(255) NOT NULL,
            PRIMARY KEY (movieId)
        );
        """
        cursor.execute(create_table_query)
        print(f"表 {table_name} 创建成功或已存在！")

        # 插入数据
        for _, row in data.iterrows():
            insert_query = f"INSERT INTO {table_name} (movieId, title, genres) VALUES (%s, %s, %s)"
            cursor.execute(insert_query, (int(row["movieId"]), row["title"], row["genres"]))
        connection.commit()
        print(f"数据成功插入到表 {table_name} 中！")
    except Exception as e:
        print(f"插入数据失败: {e}")
    finally:
        connection.close()


def load_ratings_to_mysql(csv_file_path, table_name, db_config):
    """
    从 ratings.csv 文件读取数据并插入到 MySQL 数据库的 ratings 表中。
    """
    try:
        # 读取 CSV 文件
        data = pd.read_csv(csv_file_path)
        print(f"CSV 文件 {csv_file_path} 读取成功！")

        # 强制转换数据类型
        data["userId"] = data["userId"].astype(int)
        data["movieId"] = data["movieId"].astype(int)
        data["rating"] = data["rating"].astype(float)
        data["timestamp"] = data["timestamp"].astype(int)
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
            userId INT NOT NULL,
            movieId INT NOT NULL,
            rating FLOAT NOT NULL,
            timestamp BIGINT NOT NULL,
            PRIMARY KEY (userId, movieId)
        );
        """
        cursor.execute(create_table_query)
        print(f"表 {table_name} 创建成功或已存在！")

        # 插入数据
        for _, row in data.iterrows():
            insert_query = f"INSERT INTO {table_name} (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, %s)"
            cursor.execute(insert_query, (int(row["userId"]), int(row["movieId"]), float(row["rating"]), int(row["timestamp"])))
        connection.commit()
        print(f"数据成功插入到表 {table_name} 中！")
    except Exception as e:
        print(f"插入数据失败: {e}")
    finally:
        connection.close()