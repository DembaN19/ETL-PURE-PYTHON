import pymssql
import os
import sys
from pathlib import Path
from pyhocon import ConfigFactory
from sqlalchemy import create_engine

# Chemin absolu vers le répertoire racine du projet
root_dir = 'directory-path'
if root_dir not in sys.path:
    sys.path.append(root_dir)

# Import des modules du projet
from src.utils import build_sql_pymssql
from src.setup import config_file

# Chargement de la configuration
conf = ConfigFactory.parse_file(config_file)
# Connexion SQL Server DWH
server = conf.get('db_dwh.server')
database = conf.get('db_dwh.database')
username = conf.get('db_dwh.username')
password = conf.get('db_dwh.password')

conn_str = (f"mssql+pyodbc://{username}:{password}@{server}.company_name.lan:1433/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(conn_str, fast_executemany=True)
