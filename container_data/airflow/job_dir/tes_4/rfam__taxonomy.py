##################################################
# import installed library
##################################################
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pandas import read_sql

##################################################
# import default library
##################################################
from urllib.parse import quote_plus

##################################################

##################################################
# global variable
##################################################
OUTPUT_DIRECTORY = """/home/airflow/output/tes_4"""
CONNECTIONS = {
    "mysql": {
        "driver": "pymysql",
        "credentials": {
            "mysql-rfam-public.ebi.ac.uk Rfam": {
                "host": "mysql-rfam-public.ebi.ac.uk",
                "port": "4497",
                "username": "rfamro",
                "password": "",
                "database": "Rfam"
            }
        }
    }
}

##################################################
# function to execute
##################################################
def create_url(product: str, credential_name: str) -> str:
    url = URL.create(
        f"""{product}+{CONNECTIONS[product]["driver"]}""",
        username=f"""{CONNECTIONS[product]["credentials"][credential_name]["username"]}""",
        password=quote_plus(f"""{CONNECTIONS[product]["credentials"][credential_name]["password"]}"""),  
        host=f"""{CONNECTIONS[product]["credentials"][credential_name]["host"]}""",
        port=f"""{CONNECTIONS[product]["credentials"][credential_name]["port"]}""",
        database=f"""{CONNECTIONS[product]["credentials"][credential_name]["database"]}""",
    )
    return url

##################################################
# pipeline
##################################################
def main():
    input_url = create_url("mysql","mysql-rfam-public.ebi.ac.uk Rfam")
    engine = create_engine(url=input_url)
    data = read_sql(sql=f"select * from taxonomy limit 100",con=engine,chunksize=10)
    for i, chunk in enumerate(data):
        chunk.to_csv(f"{OUTPUT_DIRECTORY}/rfam__taxonomy.csv", mode="a", header=(i==0), index=False)

##################################################
# test
##################################################
if __name__ == "__main__":
    main()