##################################################
# import installed library
##################################################
from mssql_python import connect, SQL_CHAR
from pandas import read_sql

##################################################
# import default library
##################################################
from urllib.parse import quote_plus
import csv

##################################################

##################################################
# global variable
##################################################
OUTPUT_DIRECTORY = """/home/airflow/etl_output/tes_2"""
CONNECTIONS = {
    "mssql": {
        "driver": "pymssql",
        "credentials": {
            "10.11.88.218 stg_host": {
                "host": "10.11.88.218",
                "port": "1433",
                "username": "dwhadm",
                "password": "0DSC0B$!@",
                "database": "stg_host"
            }
        }
    },
}

##################################################
# function to execute
##################################################
def create_url(product: str, credential_name: str) -> str:
    url = f"""Server={CONNECTIONS[product]["credentials"][credential_name]["host"]};Database={CONNECTIONS[product]["credentials"][credential_name]["database"]};UID={CONNECTIONS[product]["credentials"][credential_name]["username"]};PWD={CONNECTIONS[product]["credentials"][credential_name]["password"]};Encrypt=no;TrustServerCertificate=yes;"""
    return url

##################################################
# pipeline
##################################################
def main():
    input_url = create_url("mssql","10.11.88.218 stg_host")
    with connect(input_url) as connection:
        connection.setdecoding(SQL_CHAR, encoding='utf-8')
        with connection.cursor() as cursor:
            cursor.execute("select * from dbo.current_cc_scmcaccp")
            for i in range(0,10):
                data = cursor.fetchmany(size=100)
                with open(f"{OUTPUT_DIRECTORY}/dbo__current_cc_scmcaccp_{i}.txt", "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f, delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    writer.writerows(data)

##################################################
# test
##################################################
if __name__ == "__main__":
    main()