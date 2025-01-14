import pymysql

def get_db_connection():
    """Create a database connection"""
    return pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='yfg,123456',
        db='spider',
        charset='utf8'
    )

def create_user_favorite_movies_table():
    """Create the user_favorite_movies table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS user_favorite_movies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (movie_id) REFERENCES movie250(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("Table user_favorite_movies created successfully!")

    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_user_favorite_movies_table()