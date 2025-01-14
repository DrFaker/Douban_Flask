import pymysql
from datetime import datetime
from werkzeug.security import generate_password_hash
import sys


def get_db_connection():
    """创建数据库连接"""
    return pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='yfg,123456',
        charset='utf8'
    )


def create_database_and_tables():
    """创建数据库和用户表"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 创建数据库（如果不存在）
        cursor.execute("CREATE DATABASE IF NOT EXISTS spider")
        cursor.execute("USE spider")

        # 删除现有表（如果需要重建）
        cursor.execute("DROP TABLE IF EXISTS users")

        # 创建用户表 - 移除 nickname 字段
        create_users_table = """
        CREATE TABLE users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            password VARCHAR(255) NOT NULL,
            avatar_url VARCHAR(255),
            last_login DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_username (username)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_users_table)

        # 创建初始用户数据
        test_users = [
            {
                'username': 'admin',
                'password': 'admin123'
            },
            {
                'username': 'test1',
                'password': 'test123'
            },
            {
                'username': 'test2',
                'password': 'test123'
            }
        ]

        # 插入初始用户
        current_time = datetime.utcnow()
        for user in test_users:
            try:
                password_hash = generate_password_hash(user['password'])
                sql = """
                INSERT INTO users 
                    (username, password, avatar_url, last_login, created_at) 
                VALUES 
                    (%s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    user['username'],
                    password_hash,
                    '/static/assets/img/default-avatar.png',  # 默认头像
                    current_time,
                    current_time
                ))
                print(f"创建用户成功: {user['username']}")
            except Exception as e:
                print(f"创建用户 {user['username']} 时出错: {e}")

        conn.commit()
        print("数据库和表创建成功！")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def create_initial_users(cursor):
    """创建初始用户"""
    test_users = [
        {
            'username': 'admin',
            'password': 'admin123',
            'nickname': '管理员',
            'avatar_url': '/static/assets/img/default-avatar.png'
        },
        {
            'username': 'test1',
            'password': 'test123',
            'nickname': '测试用户1',
            'avatar_url': '/static/assets/img/default-avatar.png'
        },
        {
            'username': 'test2',
            'password': 'test123',
            'nickname': '测试用户2',
            'avatar_url': '/static/assets/img/default-avatar.png'
        }
    ]

    for user in test_users:
        try:
            # 检查用户是否已存在
            cursor.execute("SELECT id FROM users WHERE username = %s", (user['username'],))
            if cursor.fetchone() is None:
                # 生成密码哈希
                password_hash = generate_password_hash(user['password'])
                current_time = datetime.utcnow()

                # 插入用户数据
                sql = """
                    INSERT INTO users (username, password_hash, nickname, avatar_url, last_login, created_at) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                cursor.execute(sql, (
                    user['username'],
                    password_hash,
                    user['nickname'],
                    user['avatar_url'],
                    current_time,
                    current_time
                ))
                print(f"创建用户成功: {user['username']}")
            else:
                print(f"用户已存在: {user['username']}")

        except Exception as e:
            print(f"创建用户 {user['username']} 时出错: {e}")


# 验证表结构的函数
def verify_database():
    """验证数据库设置"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 检查数据库是否存在
        cursor.execute("SHOW DATABASES LIKE 'spider'")
        if cursor.fetchone() is None:
            print("数据库不存在，开始创建...")
            create_database_and_tables()
        else:
            print("数据库已存在")
            cursor.execute("USE spider")

            # 显示表结构
            cursor.execute("SHOW TABLES LIKE 'users'")
            if cursor.fetchone() is None:
                print("用户表不存在，开始创建...")
                create_database_and_tables()
            else:
                print("用户表已存在")

                # 显示表结构
                cursor.execute("DESCRIBE users")
                print("\n表结构:")
                for field in cursor.fetchall():
                    print(f"{field[0]}: {field[1]}")

                # 显示用户数量和列表
                cursor.execute("SELECT COUNT(*) FROM users")
                user_count = cursor.fetchone()[0]
                print(f"\n当前用户数量: {user_count}")

                cursor.execute("SELECT username FROM users")
                print("\n现有用户列表:")
                for user in cursor.fetchall():
                    print(f"用户名: {user[0]}")

    except Exception as e:
        print(f"验证数据库时出错: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print(f"开始验证数据库设置... 当前时间: {datetime.utcnow()}")
    create_database_and_tables()  # 先创建数据库和表
    print("\n验证数据库结构:")
    verify_database()  # 然后验证结果