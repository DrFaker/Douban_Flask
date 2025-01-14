from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pymysql
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import jieba
import re
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # 设置密钥，用于session加密

def get_db_connection():
    return pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='yfg,123456',
        db='spider',
        charset='utf8'
    )

# 登录验证装饰器
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('请先登录！', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# 主页路由 - 需要登录
@app.route('/')
@app.route('/index')
@login_required
def index():
    return render_template('index.html')

# 添加/删除收藏
@app.route('/toggle_favorite/<int:movie_id>', methods=['POST'])
@login_required
def toggle_favorite(movie_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 检查是否已收藏
        cursor.execute('''
            SELECT id FROM user_favorite_movies
            WHERE user_id = %s AND movie_id = %s
        ''', (session['user_id'], movie_id))

        existing = cursor.fetchone()

        if existing:
            # 取消收藏
            cursor.execute('''
                DELETE FROM user_favorite_movies
                WHERE user_id = %s AND movie_id = %s
            ''', (session['user_id'], movie_id))
            is_favorite = False
        else:
            # 添加收藏
            cursor.execute('''
                INSERT INTO user_favorite_movies (user_id, movie_id)
                VALUES (%s, %s)
            ''', (session['user_id'], movie_id))
            is_favorite = True

        conn.commit()
        return jsonify({'success': True, 'is_favorite': is_favorite})

    except Exception as e:
        print(f"Toggle favorite error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

    finally:
        cursor.close()
        conn.close()

# 个人主页路由
@app.route('/profile')
@login_required
def profile():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 获取用户信息
        cursor.execute('SELECT * FROM users WHERE id = %s', (session['user_id'],))
        user = cursor.fetchone()

        # 获取用户收藏的电影
        cursor.execute('''
            SELECT m.* FROM movie250 m
            JOIN user_favorite_movies ufm ON m.id = ufm.movie_id
            WHERE ufm.user_id = %s
        ''', (session['user_id'],))
        favorite_movies = cursor.fetchall()

        # 提取用户喜欢的电影类型
        movie_types = set()
        for movie in favorite_movies:
            # 从电影信息中提取类型
            info = movie[8]  # 假设电影类型信息在info字段中
            # 使用正则表达式提取类型信息
            types = re.findall(r'类型: (.*?)(?:\s|$)', info)
            if types:
                # 将类型字符串分割成列表并添加到集合中
                movie_types.update([t.strip() for t in types[0].split('/')])

        # 获取推荐电影
        recommended_movies = []
        if favorite_movies:
            recommended_movies = get_movie_recommendations(
                [movie[0] for movie in favorite_movies],
                cursor
            )

        return render_template('profile.html',
                             user=user,
                             favorite_movies=favorite_movies,
                             recommended_movies=recommended_movies,
                             movie_types=list(movie_types))  # 将集合转换为列表

    finally:
        cursor.close()
        conn.close()


# 允许上传的图片格式
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # 获取上传的头像
        avatar = request.files.get('avatar')
        avatar_path = None

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # 检查用户名是否已存在
            cursor.execute('SELECT id FROM users WHERE username = %s', (username,))
            if cursor.fetchone():
                flash('用户名已存在！', 'danger')
                return render_template('register.html')

            # 处理头像上传
            if avatar and allowed_file(avatar.filename):
                filename = secure_filename(f"{username}_{int(datetime.now().timestamp())}")
                ext = avatar.filename.rsplit('.', 1)[1].lower()
                avatar_filename = f"{filename}.{ext}"

                upload_dir = os.path.join('static', 'uploads', 'avatars')
                os.makedirs(upload_dir, exist_ok=True)

                avatar_path = f'/static/uploads/avatars/{avatar_filename}'
                avatar.save(os.path.join('static', 'uploads', 'avatars', avatar_filename))

            # 创建新用户
            password_hash = generate_password_hash(password)
            current_time = datetime.utcnow()

            sql = '''
                INSERT INTO users
                (username, password, avatar_url, last_login, created_at)
                VALUES
                (%s, %s, %s, %s, %s)
            '''
            cursor.execute(sql, (
                username,
                password_hash,
                avatar_path or '/static/assets/img/default-avatar.png',
                current_time,
                current_time
            ))

            conn.commit()
            flash('注册成功！请登录。', 'success')
            return redirect(url_for('login'))

        except Exception as e:
            print(f"Registration error: {str(e)}")
            flash('注册过程中发生错误！', 'danger')
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

    return render_template('register.html')

# 登录路由
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT * FROM users WHERE username = %s', (username,))
            user = cursor.fetchone()

            if user and check_password_hash(user[2], password):
                session['logged_in'] = True
                session['username'] = username
                session['user_id'] = user[0]
                session['avatar_url'] = user[5] or '/static/assets/img/default-avatar.png'

                cursor.execute('UPDATE users SET last_login = %s WHERE id = %s',
                               (datetime.utcnow(), user[0]))
                conn.commit()

                flash('登录成功！', 'success')
                return redirect(url_for('index'))
            else:
                flash('用户名或密码错误！', 'error')

        except Exception as e:
            flash('登录过程中发生错误！', 'error')
            print(f"Login error: {str(e)}")

        finally:
            cursor.close()
            conn.close()

    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('您已成功退出！', 'success')
    return redirect(url_for('login'))

def calculate_movie_similarity(movie1, movie2):
    """Calculate similarity between two movies"""
    try:
        similarity = 0

        # 1. Score similarity (25%)
        if movie1['score'] and movie2['score']:
            score_similarity = 1 - abs(float(movie1['score']) - float(movie2['score'])) / 10
            similarity += score_similarity * 0.25

        # 2. Introduction similarity (25%)
        if movie1['introduce'] and movie2['introduce']:
            intro_similarity = calculate_text_similarity(movie1['introduce'], movie2['introduce'])
            similarity += intro_similarity * 0.25

        # 3. Comments similarity (25%)
        if movie1['comments'] and movie2['comments']:
            comments_similarity = calculate_text_similarity(movie1['comments'], movie2['comments'])
            similarity += comments_similarity * 0.25

        # 4. Other info similarity (25%)
        if movie1['info'] and movie2['info']:
            info_similarity = calculate_text_similarity(movie1['info'], movie2['info'])
            similarity += info_similarity * 0.25

        return similarity

    except Exception as e:
        print(f"Error in calculate_similarity: {str(e)}")
        return 0

def calculate_text_similarity(text1, text2):
    """Calculate text similarity"""
    try:
        # Text preprocessing
        def preprocess_text(text):
            # Remove special characters
            text = re.sub(r'[^\w\s]', '', text)
            # Use jieba for word segmentation
            return ' '.join(jieba.cut(text))

        # Process texts
        processed_text1 = preprocess_text(text1)
        processed_text2 = preprocess_text(text2)

        # Use TF-IDF vectorization
        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform([processed_text1, processed_text2])

        # Calculate cosine similarity
        similarity = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]

        return similarity
    except:
        return 0

def get_movie_recommendations(favorite_movie_ids, cursor):
    try:
        # 获取用户收藏的电影信息
        favorite_movies_info = []
        for movie_id in favorite_movie_ids:
            cursor.execute('SELECT * FROM movie250 WHERE id = %s', (movie_id,))
            movie = cursor.fetchone()
            if movie:
                favorite_movies_info.append(movie)

        # 获取所有其他电影
        cursor.execute('SELECT * FROM movie250 WHERE id NOT IN (%s)' % ','.join(['%s'] * len(favorite_movie_ids)),
        tuple(favorite_movie_ids))
        all_other_movies = cursor.fetchall()

        # 计算相似度并推荐
        recommendations = []
        for other_movie in all_other_movies:
            max_similarity = 0
            # 计算与每个收藏电影的相似度
            for fav_movie in favorite_movies_info:
                similarity = calculate_movie_similarity(fav_movie, other_movie)
                max_similarity = max(max_similarity, similarity)

            recommendations.append({
                'movie': other_movie,
                'similarity': max_similarity
            })

        # 按相似度排序并返回前10个推荐
        recommendations.sort(key=lambda x: x['similarity'], reverse=True)
        return [rec['movie'] for rec in recommendations[:10]]

    except Exception as e:
        print(f"Recommendation error: {str(e)}")
        return []

@app.route('/movie/<int:movie_id>/recommendations')
def movie_recommendations(movie_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get current movie information
        cursor.execute('SELECT * FROM movie250 WHERE id = %s', (movie_id,))
        current_movie = cursor.fetchone()

        if not current_movie:
            return "Movie not found", 404

        # Process comments - convert "|" separated comments to list
        comments = current_movie[9].split('|') if current_movie[9] else []
        # Ensure only the first 5 comments are taken
        comments = comments[:5] if len(comments) > 5 else comments

        # Convert current movie data to dictionary format
        current_movie_dict = {
            'id': current_movie[0],
            'info_link': current_movie[1],
            'pic_link': current_movie[2],
            'cname': current_movie[3],
            'ename': current_movie[4],
            'score': float(current_movie[5]) if current_movie[5] else 0.0,
            'rated': current_movie[6],
            'introduce': current_movie[7],
            'info': current_movie[8],
            'comments': comments  # Now a list of comments
        }

        # Get all other movies
        cursor.execute('SELECT * FROM movie250 WHERE id != %s', (movie_id,))
        all_movies = cursor.fetchall()

        # Calculate movie similarity
        recommended_movies = []
        for movie in all_movies:
            movie_dict = {
                'id': movie[0],
                'info_link': movie[1],
                'pic_link': movie[2],
                'cname': movie[3],
                'ename': movie[4],
                'score': float(movie[5]) if movie[5] else 0.0,
                'rated': movie[6],
                'introduce': movie[7],
                'info': movie[8],
                'comments': movie[9]
            }

            # Calculate similarity
            similarity = calculate_movie_similarity(current_movie_dict, movie_dict)
            movie_dict['similarity_score'] = similarity
            recommended_movies.append(movie_dict)

        # Sort by similarity and get top 6
        recommended_movies.sort(key=lambda x: x['similarity_score'], reverse=True)
        recommended_movies = recommended_movies[:6]

        cursor.close()
        conn.close()

        return render_template('recommendations.html',
                               current_movie=current_movie_dict,
                               recommended_movies=recommended_movies)

    except Exception as e:
        print(f"Error in movie_recommendations: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        return "An error occurred, please try again later", 500

@app.route('/movie')
@login_required
def movie():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 获取所有电影
        cursor.execute('SELECT * FROM movie250')
        movies = cursor.fetchall()

        # 获取用户收藏的电影ID列表
        cursor.execute('''
            SELECT movie_id FROM user_favorite_movies
            WHERE user_id = %s
        ''', (session['user_id'],))
        favorite_movies = {row[0] for row in cursor.fetchall()}

        return render_template('movie.html',
                               movies=movies,
                               favorite_movies=favorite_movies)
    finally:
        cursor.close()
        conn.close()

@app.route('/score')
def score():
    score = []
    num = []
    conn = get_db_connection()
    cursor = conn.cursor()
    sql = 'select score,count(score) from movie250 group by score'
    cursor.execute(sql)
    data = cursor.fetchall()
    for item in data:
        score.append(str(item[0]))
        num.append(item[1])
    cursor.close()
    conn.close()
    return render_template("score.html", score=score, num=num)

@app.route('/word')
def word():
    return render_template("word.html")

if __name__ == '__main__':
    app.run()