from data import Data
from data_base_work import DataBaseWork
from DBManager import DBManager
import os


def new_database():
    """
    Creating a new database
    """

    host = 'localhost'
    database = 'hh'
    user = 'postgres'
    port = '5432'
    password = os.getenv('DB_Password')
  
    # Creating an object for loading data into the database
    hh_data = Data()

    # Database connection
    data_base_hh = DataBaseWork(db_host=host, db_port=port, db_name=database, db_user=user, db_password=password)
    
    data_base_hh.connect()

    data_base_hh.create_tables()

    # Adding company data
    hh_data.get_companies()
    for index in range(len(hh_data.company_name)):
        data_base_hh.fill_company(company_id=hh_data.company_id[index],
                             name=hh_data.company_name[index],
                             vacancies_count=hh_data.vacancies_count[index]
                             )
    # Adding information about vacancies
    hh_data.get_vacancies()
    for index in range(len(hh_data.vacancies_id)):
         data_base_hh.fill_vacancy(vacancy_id=hh_data.vacancies_id[index],
                             company_id=hh_data.vacancies_company_id[index],
                             name=hh_data.vacancies_name[index],
                             salary_from=hh_data.salary_from[index],
                             salary_to=hh_data.salary_to[index],
                             currency=hh_data.currency[index],
                             url=hh_data.url[index]
                             )
    # Closing the database
    data_base_hh.disconnect()


def menu():
    """
    Function for user interaction
    """

    try:
        user_answer = int(input('Please select the desired action:\n'
                           '1: Get a list of all companies and the number of vacancies for each company\n'
                           '2: Get a list of all vacancies with the name of the company name of the vacancy and salaries and a link to the vacancy\n'
                           '3: Get the average salary for vacancies\n'
                           '4: Get a list of all vacancies with a salary above the average for all vacancies\n'
                           '5: Get a list of all vacancies whose title contains the specified words: \n'))
      
        if user_answer in range(6):
            if user_answer == 1:
                for key, item in manager_db.get_companies_and_vacancies_count().items():
                    print(f'Company: {key}, Number of vacancies: {item}')

            elif user_answer == 2:
                for key, item in manager_db.get_all_vacancies().items():
                    word_from = ' from '
                    word_to = ' to '
                    if item[2] == 0:
                        item[2] = ''
                        word_from = ''
                    if item[3] == 0:
                        item[3] = ''
                        word_to = ''
                    print(f'Company: {item[0]}, job vacancy: {item[1]}, salary:'
                          f'{word_from}{item[2]}{word_to}{item[3]} {item[4]}, link: {item[5]}')

            elif user_answer == 3:
                for key, item in manager_db.get_avg_salary().items():
                    if int(item[0]) == 0:
                        print(f'Job vacancy: {key}, average salary: {item[1]}')
                    else:
                        print(f'Job vacancy: {key}, average salary: {int(item[0])} {item[1]}')

            elif user_answer == 4:
                for key, item in manager_db.get_vacancies_with_higher_salary().items():
                    if int(item[0]) == 0:
                        print(f'Job vacancy: {key}, average salary: {item[1]}')
                    else:
                        print(f'Job vacancy: {key}, average salary: {int(item[0])} {item[1]}')

            elif user_answer == 5:
                key_word = input('Enter a keyword to search: ')
                for key, item in manager_db.get_vacancies_with_keyword(key_word).items():
                    word_from = ' from '
                    word_to = ' to '
                    if item[2] == 0:
                        item[2] = ''
                        word_from = ''
                    if item[3] == 0:
                        item[3] = ''
                        word_to = ''
                    print(
                        f'Company: {item[0]}, job vacancy: {item[1]}, salary:'
                        f'{word_from}{item[2]}{word_to}{item[3]} {item[4]}, link: {item[5]}')
        else:
            print('Action is not correct')
    except ValueError as e:
        print(f'Action is not correct: {e}')


# Creating a new database
new_database()

manager_db = DBManager()

# Starting a dialogue with the user
menu()
