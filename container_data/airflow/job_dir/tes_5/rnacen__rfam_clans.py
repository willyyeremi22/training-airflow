##################################################
# import installed library
##################################################
import psycopg

##################################################
# import default library
##################################################
from csv import writer as csv_writer, QUOTE_ALL

##################################################

##################################################
# global variable
##################################################
OUTPUT_DIRECTORY = """/home/airflow/etl_output/tes_5"""
CONNECTIONS = {
    "postgresql": {
        "driver": "psycopg",
        "credentials": {
            "hh-pgsql-public.ebi.ac.uk pfmegrnargs": {
                "host": "hh-pgsql-public.ebi.ac.uk",
                "port": "5432",
                "username": "reader",
                "password": "NWDMCE5xdipIjRrp",
                "database": "pfmegrnargs"
            }
        }
    }
}

##################################################
# function to execute
##################################################
def create_url(product: str, credential_name: str) -> str:
    url = f"""host={CONNECTIONS[product]["credentials"][credential_name]["host"]} port={CONNECTIONS[product]["credentials"][credential_name]["port"]} user={CONNECTIONS[product]["credentials"][credential_name]["username"]} password={CONNECTIONS[product]["credentials"][credential_name]["password"]} dbname={CONNECTIONS[product]["credentials"][credential_name]["database"]}"""
    return url

##################################################
# pipeline
##################################################
def main():
    input_url = create_url("postgresql","hh-pgsql-public.ebi.ac.uk pfmegrnargs")
    with psycopg.connect(conninfo=input_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("select * from rnacen.rfam_clans")
            for i in range(0,10):
                data = cursor.fetchmany(size=100)
                with open(f"{OUTPUT_DIRECTORY}/rnacen__rfam_clans.csv", "a", newline="", encoding="utf-8") as f:
                    writer = csv_writer(f, delimiter="|", quotechar='"', quoting=QUOTE_ALL)
                    writer.writerows(data)

##################################################
# test
##################################################
if __name__ == "__main__":
    main()