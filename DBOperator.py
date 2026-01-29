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

    def exists_query_1(self):
        """Врачи, у которых есть премия более 5000"""
        SQL_QUERY = """SELECT d.Surname, d.Name, d.Salary, d.Premium
                    FROM dbo.Doctors d
                    WHERE EXISTS (SELECT 1
                                    FROM dbo.Doctors d2
                                    WHERE d2.id = d.id
                                    AND d2.Premium > 5000)
                    ORDER BY d.Premium DESC;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def exists_query_2(self):
        """Отделения, у которых есть палаты с количеством мест более 3"""
        SQL_QUERY = """SELECT dep.Name, dep.Building, w.Name as WardName, w.Place
                        FROM dbo.Departments dep
                        WHERE EXISTS (SELECT 1
                                        FROM dbo.Wards w
                                        WHERE w.DepartmentId = dep.id
                                        AND w.Place > 3)
                        ORDER BY dep.Building;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def any_query(self):
        """Врачи, чья зарплата больше любой зарплаты врачей с премией меньше 2000"""
        SQL_QUERY = """SELECT d.Surname, d.Name, d.Salary, d.Premium
                        FROM dbo.Doctors d
                        WHERE d.Salary > ANY (
                            SELECT Salary
                            FROM dbo.Doctors
                            WHERE Premium < 2000)
                        ORDER BY d.Salary DESC;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def some_query(self):
        """Спонсоры, которые сделали пожертвования в любое отделение в 1корпусе"""
        SQL_QUERY = """SELECT s.Name as SponsorName, d.Name as DepartmentName, don.Amount, don.Date
                        FROM dbo.Sponsors s
                        INNER JOIN dbo.Donations don ON s.id = don.SponsorId
                        INNER JOIN dbo.Departments d ON don.DepartmentId = d.id
                        WHERE d.id = SOME (SELECT id 
                                            FROM dbo.Departments 
                                            WHERE Building = 1)
                        ORDER BY don.Amount DESC;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()
