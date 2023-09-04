import psycopg2
import os


class DBManager:
    def get_companies_and_vacancies_count(self):
        """
        The method gets a list of all companies and the number of vacancies for each company
        """

        conn = psycopg2.connect(
            dbname='hh',
            user='postgres',
            password=os.getenv('DB_Password'),
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        # Here we get a list of companies and the number of vacancies for each company
        query = """
                SELECT name, vacancies_count
                FROM companies
            """
        cursor.execute(query)
        results = cursor.fetchall()

        # Here we create a dictionary with the results
        company_vacancies = {}
        for result in results:
            company_name = result[0]
            vacancies_count = result[1]
            company_vacancies[company_name] = vacancies_count

        cursor.close()
        conn.close()

        return company_vacancies

    def get_all_vacancies(self):
        """
        The method receives a list of all vacancies with the company name, vacancy name and salary, and a link to the vacancy
        """

        conn = psycopg2.connect(
            dbname='hh',
            user='postgres',
            password=os.getenv('DB_Password'),
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        # Here we get a list of companies and the number of vacancies for each company
        query = """
                SELECT id, company_id, name, salary_from, salary_to, currency, url
                FROM vacancies
            """
        cursor.execute(query)
        results = cursor.fetchall()

        # Here we create a dictionary with results
        vacancies = {}
        for result in results:
            vacancies[result[0]] = [result[1], result[2], result[3], result[4], result[5], result[6]]

        cursor.close()
        conn.close()

        return vacancies

    def get_avg_salary(self):
        """
        The method gets the average salary for vacancies
        """

        conn = psycopg2.connect(
            dbname='hh',
            user='postgres',
            password=os.getenv('DB_Password'),
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        # Here we get a list of companies and the number of vacancies for each company
        query = """
                SELECT name, AVG((salary_from + salary_to) / 2) as avg_salary, currency
                FROM vacancies
                GROUP BY name, currency
            """
        cursor.execute(query)
        results = cursor.fetchall()

        # Here we create a dictionary with results
        average_salary = {}
        for result in results:
            avg_salary[result[0]] = [result[1], result[2]]

        cursor.close()
        conn.close()

        return average_salary

    def get_vacancies_with_higher_salary(self):
        """
        The method receives a list of all vacancies whose salary is higher than the average for all vacancies
        """

        conn = psycopg2.connect(
            dbname='hh',
            user='postgres',
            password=os.getenv('DB_Password'),
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        # Here we get a list of companies and the number of vacancies for each company
        query = """
                SELECT name, (salary_from + salary_to) / 2, currency
                FROM vacancies
                WHERE (salary_from + salary_to) / 2 > (
                  SELECT AVG((salary_from + salary_to) / 2)
                  FROM vacancies
                )
            """
        cursor.execute(query)
        results = cursor.fetchall()

        # Here we create a dictionary with results
        higher_salary_vacancies = {}
        for result in results:
            higher_salary_vacancies[result[0]] = [result[1], result[2]]

        cursor.close()
        conn.close()

        return higher_salary_vacancies

    def get_vacancies_with_keyword(self, key_word):
        """
        The method receives a list of all vacancies whose title contains the words passed to the method, for example - python
        """

        conn = psycopg2.connect(
            dbname='hh',
            user='postgres',
            password=os.getenv('DB_Password'),
            host='localhost',
            port='5432'
        )

        cursor = conn.cursor()

        # Here we get a list of companies and the number of vacancies for each company
        query = f"SELECT id, company_id, name, salary_from, salary_to, currency, url " \
                f"FROM vacancies " \
                f"WHERE name LIKE '%{key_word.lower()}%' or name LIKE '%{key_word.capitalize()}%'"
        cursor.execute(query)
        results = cursor.fetchall()

        # Here we create a dictionary with results
        keyword_vacancies = {}
        for result in results:
            keyword_vacancies[result[0]] = [result[1], result[2], result[3], result[4], result[5], result[6]]

        cursor.close()
        conn.close()

        return keyword_vacancies
