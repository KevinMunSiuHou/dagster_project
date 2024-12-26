import os
from jinja2 import Template

def read_sql_file(file_name, variables):
    'C:/Users/Kevin Mun/Desktop/Project/dagster/sql/query.sql'
    with open(os.getcwd().replace('\\', '/') + f"/dagster_project/sql/{file_name}.sql", "r", encoding="utf-8") as file:
        sql_template = Template(file.read())
        rendered_sql = sql_template.render(**variables)
        return rendered_sql