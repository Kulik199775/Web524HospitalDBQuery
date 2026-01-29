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

    def right_join_query(self):
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

    def rows_to_dict_list(self, rows):
        """Преобразование результатов запроса в список словарей"""
        if not rows:
            return []

        result = []
        columns = [column[0] for column in self.cursor.description]

        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                if value is None:
                    row_dict[col] = None
                elif isinstance(value, (int, float, str, bool)):
                    row_dict[col] = value
                else:
                    row_dict[col] = str(value)
            result.append(row_dict)
        return result

    def save_to_json(self, rows, query_name, filename='hospital_queries.json'):
        """Сохранение результатов в JSON файл"""
        try:
            data_list = self.rows_to_dict_list(rows)
            self.cursor.execute("SELECT CONVERT(VARCHAR, GATDATE(), 120)")
            current_time = self.cursor.fetchone()[0]

            save_data = {
                'query_name': query_name,
                'timestamp': current_time,
                'row_count': len(rows),
                'data': data_list
            }

            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    file_content = f.read().strip()
                    if file_content:
                        all_data = json.loads(file_content)
                    else:
                        all_data = {}
            except:
                all_data = {}

            if 'queries' not in all_data:
                all_data['queries'] = []

            all_data['queries'].append(save_data)

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            print(f'Результаты запроса {query_name} сохранены в {filename}')
            return True

        except Exception as ex:
            print(f'Ошибка при сохранении в JSON: {ex}')
            return False

    def execute_all_queries_and_save(self, filename="hospital_results.json"):
        """Выполнение всех запросов и сохранение результатов"""
        queries = [
            ("EXISTS_1 - Врачи с премией > 5000", self.exists_query_1),
            ("EXISTS_2 - Отделения с палатами > 3 мест", self.exists_query_2),
            ("ANY - Врачи с зарплатой больше любой", self.any_query),
            ("SOME - Спонсоры 1 корпуса", self.some_query),
            ("ALL - Врачи с зарплатой больше всех", self.all_query),
            ("Combined - ANY/ALL сочетание", self.combined_query),
            ("UNION - Спонсоры и обследования", self.union_query),
            ("UNION ALL - Врачи и отделения", self.union_all_query),
            ("INNER JOIN - Врачи и обследования", self.inner_join_query),
            ("LEFT JOIN - Все врачи", self.left_join_query),
            ("RIGHT JOIN - Все обследования", self.right_join_query),
            ("FULL JOIN - Все врачи и обследования", self.full_join_query)
        ]

        print("=" * 40)
        print("Выполнение всех запросов и сохранение в JSON")
        print("=" * 40)

        for query_name, query_func in queries:
            print(f"\nВыполняем запрос: {query_name}")

            try:
                results = query_func()
                print(f"Найдено записей: {len(results)}")

                if len(results) > 0:
                    print("Первые 3 записи:")
                    for i in range(min(3, len(results))):
                        print(f"  {i + 1}. {results[i]}")

                self.save_to_json(results, query_name, filename)

            except Exception as e:
                print(f"Ошибка при выполнении запроса '{query_name}': {e}")

        print("\n" + "=" * 40)
        print("Все запросы успешно выполнены")
        print("=" * 40)