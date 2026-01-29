import json
import pyodbc
from dotenv import load_dotenv

class HospitalDB:
    def __init__(self, server, database, username, password):
        """Инициализация подключения к MSSQL Server"""
        self.connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PASSWORD={password}"
        )
        self.conn = None
        self.connect()

    def connect(self):
        """Установка соединения с БД"""
        try:
            self.conn = pyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            print(f'Подключение к БД успешно выполнено')
        except Exception as ex:
            print(f'Ошибка подключения: {ex}')

    def close(self):
        """Закрытие соединения"""
        if self.conn:
            self.cursor.close()
            self.conn.close()

