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

    def all_query(self):
        """Врачи, чья зарплата больше всех врачей с премией 0"""
        SQL_QUERY = """SELECT d.Surname, d.Name, d.Salary, d.Premium
                        FROM dbo.Doctors d
                        WHERE d.Salary > ALL (SELECT Salary 
                                                FROM dbo.Doctors 
                                                WHERE Premium = 0)
                        ORDER BY d.Salary;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def combined_query(self):
        """Врачи, чья зарплата больше всех враче с премией меньше 1000
        и у которых есть хоть одно обследование"""
        SQL_QUERY = """SELECT d.Surname, d.Name, d.Salary, d.Premium
                        FROM dbo.Doctors d
                        WHERE d.Salary > ALL (SELECT Salary 
                                                FROM dbo.Doctors 
                                                WHERE Premium < 1000)
                        AND EXISTS (SELECT 1 
                            FROM dbo.DoctorsExaminations de 
                            WHERE de.DoctorId = d.id)
                        ORDER BY d.Salary DESC;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def union_query(self):
        """Объединение имен спонсоров и названий обследований"""
        SQL_QUERY = """SELECT Name FROM dbo.Sponsors
                        UNION
                        SELECT Name FROM dbo.Examinations
                        ORDER BY Name;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def union_all_query(self):
        """Объединение всех врачей и отделений (все строки, включая дубли)"""
        SQL_QUERY = """SELECT 'Doctor' as Type, Surname + ' ' + Name as FullName, 
                        CAST(Salary as nvarchar(20)) as Details
                        FROM dbo.Doctors
                        UNION ALL
                        SELECT 'Department' as Type, Name as FullName, 
                                'Building ' + CAST(Building as nvarchar(10)) as Details
                        FROM dbo.Departments
                        ORDER BY Type, FullName;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def inner_join_query(self):
        """Врачи и их обследования"""
        SQL_QUERY = """ SELECT d.Surname, d.Name as DoctorName,
                            e.Name as ExaminationName,
                            de.StartTime, de.EndTime
                        FROM dbo.Doctors d
                        INNER JOIN dbo.DoctorsExaminations de ON d.id = de.DoctorId
                        INNER JOIN dbo.Examination e ON de.ExaminationId = e.id
                        ORDER BY d.Surname, de.StartTime;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def left_join_query(self):
        """Все врачи и их обследования"""
        SQL_QUERY = """SELECT d.Surname, d.Name as DoctorName,
                            e.Name as ExaminationName,
                            de.StartTime, de.EndTime
                        FROM dbo.Doctors d
                        LEFT JOIN dbo.DoctorsExaminations de ON d.id = de.DoctorId
                        LEFT JOIN dbo.Examinations e ON de.ExaminationId = e.id
                        ORDER BY d.Surname;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def right_jon_query(self):
        """Все обследования и врачи, которые их проводят"""
        SQL_QUERY = """SELECT e.Name as ExaminationName,
                            d.Surname, d.Name as DoctorName,
                            de.StartTime, de.EndTime
                        FROM dbo.Examinations e
                        RIGHT JOIN dbo.DoctorsExaminations de ON e.id = de.ExaminatoinId
                        RIGHT JOIN dbo.Doctors d ON de.DoctorId = d.id
                        WHERE e.Name IS NOT NULL
                        ORDER BY e.Name, d.Surname;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()

    def full_join_query(self):
        """Все врачи и все обследования"""
        SQL_QUERY = """SELECT d.Surname, d.Name as DoctorName, 
                            e.Name as ExaminationName,
                            de.StartTime, de.EndTime,
                            'Doctor-Examination' as JoinType
                        FROM dbo.Doctors d
                        LEFT JOIN dbo.DoctorsExaminations de ON d.id = de.DoctorId
                        LEFT JOIN dbo.Examinations e ON de.ExaminationId = e.id
        
                        UNION
        
                        SELECT NULL as Surname, NULL as DoctorName,
                                e.Name as ExaminationName,
                                NULL as StartTime, NULL as EndTime,
                                'Examination only' as JoinType
                        FROM dbo.Examinations e
                        WHERE e.id NOT IN (SELECT DISTINCT ExaminationId 
                        FROM dbo.DoctorsExaminations 
                        WHERE ExaminationId IS NOT NULL)
                        ORDER BY ExaminationName, Surname;"""
        self.cursor.execute(SQL_QUERY)
        return self.cursor.fetchall()