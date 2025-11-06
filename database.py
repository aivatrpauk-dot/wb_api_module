# database.py
import sqlite3


def init_db():
    """Инициализирует базу данных и создает таблицу пользователей."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            api_key TEXT,
            tax_rate REAL,
            google_sheet_link TEXT,
            shop_name TEXT
        )
    """)
    conn.commit()
    conn.close()


def user_exists(user_id):
    """Проверяет, существует ли пользователь в базе."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def add_user(user_id):
    """Добавляет нового пользователя в базу."""
    if not user_exists(user_id):
        conn = sqlite3.connect("users.db")
        cur = conn.cursor()
        cur.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        conn.close()


def update_api_key(user_id, api_key):
    """Обновляет API-ключ пользователя."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("UPDATE users SET api_key = ? WHERE user_id = ?",
                (api_key, user_id))
    conn.commit()
    conn.close()


def update_tax_rate(user_id, tax_rate):
    """Обновляет налоговую ставку пользователя."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("UPDATE users SET tax_rate = ? WHERE user_id = ?",
                (tax_rate, user_id))
    conn.commit()
    conn.close()


def get_user_data(user_id):
    """Получает все данные пользователя."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute(
        "SELECT api_key, tax_rate, google_sheet_link, shop_name FROM users WHERE user_id = ?", (user_id,))
    data = cur.fetchone()
    conn.close()
    return data if data else (None, None, None, None)


def update_google_sheet_link(user_id, link):
    """Обновляет ссылку на Google Таблицу."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET google_sheet_link = ? WHERE user_id = ?", (link, user_id))
    conn.commit()
    conn.close()


def update_shop_name(user_id: int, shop_name: str):
    """Обновляет название магазина."""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("UPDATE users SET shop_name = ? WHERE user_id = ?",
                (shop_name, user_id))
    conn.commit()
    conn.close()


def get_product_costs(user_id: int):
    """Возвращает словарь с себестоимостью товаров по артикулам"""
    # Реализация получения себестоимости из базы данных
    pass


def get_tax_rate(user_id: int):
    """Возвращает налоговую ставку для магазина"""
    conn = sqlite3.connect("users.db")
    cur = conn.cursor()
    cur.execute("SELECT tax_rate FROM users WHERE user_id = ?", (user_id,))
    data = cur.fetchone()
    conn.close()
    return data if data else None
