##################################################
# import installed library
##################################################
from mssql_python import connect, SQL_CHAR

##################################################
# import default library
##################################################
from csv import writer as csv_writer, QUOTE_MINIMAL, QUOTE_ALL

##################################################

##################################################
# global variable
##################################################
OUTPUT_DIRECTORY = """/home/airflow/etl_output/tes_4"""
CONNECTIONS = {
    "mssql": {
        "driver": "mssql-python",
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
    url:str = f"""Server={CONNECTIONS[product]["credentials"][credential_name]["host"]};Database={CONNECTIONS[product]["credentials"][credential_name]["database"]};UID={CONNECTIONS[product]["credentials"][credential_name]["username"]};PWD={CONNECTIONS[product]["credentials"][credential_name]["password"]};Encrypt=no;TrustServerCertificate=yes;"""
    return url

##################################################
# pipeline
##################################################
def main():
    input_url = create_url(product="mssql",credential_name="10.11.88.218 stg_host")
    with connect(input_url) as connection:
        connection.setdecoding(SQL_CHAR, encoding='utf-8')
        with connection.cursor() as cursor:
            cursor.execute("select * from dbo.current_cc_scmcaccp")
            for i in range(0,10):
                columns_description: list[tuple[str]] = cursor.description
                columns_name: list[tuple[str]] = [(column_description[0] for column_description in columns_description)]
                with open(f"{OUTPUT_DIRECTORY}/dbo__current_cc_scmcaccp_{i}.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv_writer(f, delimiter="|", quotechar='"', quoting=QUOTE_MINIMAL)
                    writer.writerows(columns_name)
                data: list[tuple[str]] = cursor.fetchmany(size=100)
                with open(f"{OUTPUT_DIRECTORY}/dbo__current_cc_scmcaccp_{i}.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv_writer(f, delimiter="|", quotechar='"', quoting=QUOTE_ALL)
                    writer.writerows(data)

##################################################
# test
##################################################
if __name__ == "__main__":
    main()