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
OUTPUT_DIRECTORY = """/home/airflow/output/tes_2"""
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
def create_url(product, credential_name) -> str:
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
    input_url = create_url("mssql","10.11.88.218 stg_host")
    engine = create_engine(url=input_url)
    data = read_sql(sql=f"select top 100 * from dbo.current_cc_scmaccp",con=engine,chunksize=10)
    for i, chunk in enumerate(data):
        chunk.to_csv(f"{OUTPUT_DIRECTORY}/dbo__current_cc_scmaccp.csv", mode="a", header=(i==0), index=False)

##################################################
# test
##################################################
if __name__ == "__main__":
    main()