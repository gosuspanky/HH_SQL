def print_vacancies(vac_list):
    for vac in vac_list:
        if vac['Salary_from'] != 0 and vac['Salary_to'] != 0:
            print(f'Наименование компании: {vac['Company_name']}\n'
                  f'Наименование вакансии: {vac['Vacancy_name']}\n'
                  f'Предлагаемая З/П: от {vac['Salary_from']} {vac['Currency']} '
                  f'до {vac['Salary_to']} {vac['Currency']}\n'
                  f'Ссылка на вакансию: {vac['Vacancy_url']}\n'
                  )
        elif vac['Salary_from'] != 0 and vac['Salary_to'] == 0:
            print(f'Наименование компании: {vac['Company_name']}\n'
                  f'Наименование вакансии: {vac['Vacancy_name']}\n'
                  f'Предлагаемая З/П: от {vac['Salary_from']} {vac['Currency']}\n'
                  f'Ссылка на вакансию: {vac['Vacancy_url']}\n'
                  )
        elif vac['Salary_from'] == 0 and vac['Salary_to'] != 0:
            print(f'Наименование компании: {vac['Company_name']}\n'
                  f'Наименование вакансии: {vac['Vacancy_name']}\n'
                  f'Предлагаемая З/П: до {vac['Salary_to']} {vac['Currency']}\n'
                  f'Ссылка на вакансию: {vac['Vacancy_url']}\n'
                  )
        elif vac['Salary_from'] == 0 and vac['Salary_to'] == 0:
            print(f'Наименование компании: {vac['Company_name']}\n'
                  f'Наименование вакансии: {vac['Vacancy_name']}\n'
                  f'Предлагаемая З/П не указана\n'
                  f'Ссылка на вакансию: {vac['Vacancy_url']}\n'
                  )
