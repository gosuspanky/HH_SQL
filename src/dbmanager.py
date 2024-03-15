import psycopg2

from config import config
from src.parser import HeadHunterData


class DBManager:

    def __init__(self, params: dict):
        self.params = params
        self.conn = ''
        self.cursor = ''

    def create_connection(self, dbname):
        self.conn = psycopg2.connect(dbname=dbname, **self.params)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def create_database(self, dbname):
        try:
            self.cursor.execute(f'CREATE DATABASE {dbname}')
        except psycopg2.errors.DuplicateDatabase:
            self.cursor.execute(f'DROP DATABASE {dbname}')
            self.cursor.execute(f'CREATE DATABASE {dbname}')

    def create_tables(self):
        self.cursor.execute("""
                CREATE TABLE employers (
                        employer_id INTEGER PRIMARY KEY,
                        employer_title VARCHAR(255) NOT NULL,
                        employer_url TEXT NOT NULL,
                        open_vacancies INTEGER NOT NULL
                        )
        """)

        self.cursor.execute("""
                CREATE TABLE vacancies (
                        id SERIAL PRIMARY KEY,
                        employer_title VARCHAR(255) NOT NULL,
                        vacancies_title TEXT NOT NULL,
                        area VARCHAR(255) NOT NULL,
                        salary_from INTEGER NOT NULL,
                        salary_to INTEGER NOT NULL,
                        currency VARCHAR(255) NOT NULL,
                        url TEXT NOT NULL
                        )
        """)

    def insert_data(self, emp_data, vac_data):
        for emp in emp_data:
            self.cursor.execute("""
                    INSERT INTO employers (employer_id, employer_title, employer_url, 
                    open_vacancies)
                    VALUES (%s, %s, %s, %s)             
            """, (
                emp['id'], emp['name'], emp['url'], emp['open_vacancies']
            )
                                )

        for vac in vac_data:
            self.cursor.execute("""
                               INSERT INTO vacancies (employer_title, vacancies_title, area, 
                                                      salary_from, salary_to, currency, url)
                               VALUES (%s, %s, %s, %s, %s, %s, %s)
                               RETURNING id              
                       """, (
                vac['employer_name'], vac['name'], vac['area'],
                vac['salary_from'], vac['salary_to'], vac['currency'], vac['url']
            )
                                )

    def get_companies_and_vacancies_count(self):
        """
         получает список всех компаний и количество вакансий у каждой компании.
        :return:
        """
        companies_list = []

        self.cursor.execute(
            """
            SELECT employer_title, open_vacancies
            FROM employers
            """
        )

        employers = self.cursor.fetchall()

        for row in employers:
            employer = {
                'Name': row[0],
                'Vacancies_count': row[1]
            }

            companies_list.append(employer)

        return companies_list

    def get_all_vacancies(self):
        """
        получает список всех вакансий
        с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        :return:
        """
        vacancies_list = []

        self.cursor.execute(
            """
            SELECT employer_title, vacancies_title, salary_from, salary_to, currency, url
            FROM vacancies
            """
        )

        vacancies = self.cursor.fetchall()

        for row in vacancies:

            vacancy = {
                'Company_name': row[0],
                'Vacancy_name': row[1],
                'Salary_from': row[2],
                'Salary_to': row[3],
                'Currency': row[4],
                'Vacancy_url': row[5]
            }

            vacancies_list.append(vacancy)

        return vacancies_list

    def get_avg_salary(self):
        """
        получает среднюю зарплату по вакансиям.
        :return:
        """
        salaries_list = []
        self.cursor.execute(
            """
            SELECT salary_from, salary_to, currency
            FROM vacancies
            """
        )
        salaries = self.cursor.fetchall()
        for row in salaries:

            salary = 0

            if row[0] == 0 and row[1] != 0:
                if row[2] == 'USD':
                    salary = row[1] * 92
                elif row[2] == "RUR":
                    salary = row[1]
            elif row[0] != 0 and row[1] == 0:
                if row[2] == 'USD':
                    salary = row[0] * 92
                elif row[2] == "RUR":
                    salary = row[0]

            salaries_list.append(salary)

        avg_salary = sum(salaries_list) / len(salaries_list)

        return round(avg_salary, 2)

    def get_vacancies_with_higher_salary(self):
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        :return:
        """
        average_salary = self.get_avg_salary()
        vacancies_list = self.get_all_vacancies()
        sorted_vacancies_list = []

        for vac in vacancies_list:
            if vac['Salary_from'] == 0 and vac['Salary_to'] != 0:
                if vac['Salary_to'] >= average_salary:
                    sorted_vacancies_list.append(vac)
            elif vac['Salary_from'] != 0 and vac['Salary_to'] == 0:
                if vac['Salary_from'] >= average_salary:
                    sorted_vacancies_list.append(vac)

        return sorted_vacancies_list

    def get_vacancies_with_keyword(self, keyword):
        """
        получает список всех вакансий, в названии которых
        содержатся переданные в метод слова, например python.

        :param keyword: переданные в метод слова
        :return:
        """
        vacancies_list = self.get_all_vacancies()
        sorted_vac_list = []

        for vac in vacancies_list:
            if keyword in vac['Vacancy_name']:
                sorted_vac_list.append(vac)

        return sorted_vac_list


if __name__ == '__main__':
    params = config()

    dbmanager = DBManager(params)

    # dbmanager.create_connection('postgres')
    # dbmanager.create_database('hh_parser')
    # dbmanager.close_connection()

    # hh_data = HeadHunterData()
    # hh_data.get_employers()
    # hh_data.new_employers_dicts()
    # hh_data.get_vacancies_from_emp()
    # hh_data.new_vacancies_dicts()

    # dbmanager.create_connection('hh_parser')
    # dbmanager.create_tables()
    # dbmanager.insert_data(hh_data.new_emp_list, hh_data.new_vac_list)
    # dbmanager.close_connection()

    dbmanager.create_connection('hh_parser')
    # dbmanager.get_companies_and_vacancies_count()
    # print(dbmanager.get_all_vacancies())
    # print(dbmanager.get_avg_salary())
    # print(dbmanager.get_vacancies_with_higher_salary())
    # print(dbmanager.get_vacancies_with_higher_salary())
    print(dbmanager.get_vacancies_with_keyword('Продавец'))
    dbmanager.close_connection()
