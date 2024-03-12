import json
from abc import ABC, abstractmethod
import requests
import time


class AbstractAPI(ABC):
    """
    Абстрактный метод для класса получения данных по API
    """

    @abstractmethod
    def get_employers(self):
        pass


class HeadHunterData(AbstractAPI):
    """
    Класс получения данных по API
    """

    def __init__(self):
        # self.companies = ['яндекс', '2gis']
        self.employers = []
        self.new_emp_list = []
        self.vacancies = []
        self.new_vac_list = []

    def get_employers(self):
        """
        Метод получения данных по определенным параметрам с сайта HH
        :return:
        """

        self.employers.clear()

        params = {
            "area": 113,
            'per_page': 10,
            'only_with_vacancies': True
        }

        req = requests.get(f'https://api.hh.ru/employers', params)
        json_obj = req.content.decode()

        data = json.loads(json_obj)

        self.employers.extend(data['items'])

        return self.employers

    def new_employers_dicts(self):
        self.new_emp_list.clear()
        for emp in self.employers:
            emp_dict = {
                'id': f'{emp['id']}',
                'name': f'{emp['name']}',
                'url': f'{emp['alternate_url']}',
                'vacancies_url': f'{emp['vacancies_url']}',
                'open_vacancies': emp['open_vacancies']
            }

            self.new_emp_list.append(emp_dict)

        return self.new_emp_list

    def get_vacancies_from_emp(self):
        for emp in self.new_emp_list:
            req = requests.get(f'{emp['vacancies_url']}')

            json_obj = req.content.decode()

            data = json.loads(json_obj)

            self.vacancies.extend(data['items'])

            time.sleep(0.25)

        return self.vacancies

    def new_vacancies_dicts(self):
        self.new_vac_list.clear()
        for vac in self.vacancies:
            name = f'{vac['name']}'
            area = f'{vac['area']['name']}'
            currency = f'{vac['name']}'
            url = f'{vac['alternate_url']}'
            employer_name = f'{vac['employer']['name']}'

            if vac['salary'] is not None:
                if vac['salary']['from'] is not None:

                    salary_from = vac['salary']['from']
                else:
                    salary_from = 0

                if vac['salary']['to'] is not None:
                    salary_to = vac['salary']['to']
                else:
                    salary_to = 0
            else:

                salary_from, salary_to = 0, 0

            vac_dict = {
                'name': name,
                'employer_name': employer_name,
                'area': area,
                'salary_from': salary_from,
                'salary_to': salary_to,
                'currency': currency,
                'url': url
            }

            self.new_vac_list.append(vac_dict)

        return self.new_vac_list


if __name__ == '__main__':
    x = HeadHunterData()
    x.get_employers()
    # print(x.employers)
    x.new_employers_dicts()
    # print(x.new_emp_list)
    x.get_vacancies_from_emp()
    # print(x.vacancies)
    x.new_vacancies_dicts()
    print(x.new_vac_list)
