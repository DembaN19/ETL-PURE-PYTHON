from dotenv import load_dotenv
from pathlib import Path
import sys
from pathlib import Path

# Add parent directory to Python path
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.utils import *
from src.setup import *
from pyhocon import ConfigFactory
from decorators.decrypt import decrypt_params

# Charger les variables d'environnement
load_dotenv()

conf = ConfigFactory.parse_file(config_file)
# Connexion SQL Server DWH
server = conf.get('db_dwh.server')
database = conf.get('db_dwh.database_ods')
username = conf.get('db_dwh.username')
password = conf.get('db_dwh.password')

# Connexion SQL Server BC
server_bc = conf.get('db_bc.server')
database_bc = conf.get('db_bc.database')
username_bc = conf.get('db_bc.username')
password_bc = conf.get('db_bc.password')



SQLSERVER_CONN_DWH =  build_sql_sqlalchemy_dwh()
SQLSERVER_CONN_BC = build_sql_sqlalchemy_bc()



# Créer les dossiers si besoin
Path("data_lake/raw").mkdir(parents=True, exist_ok=True)
Path("data_lake/ods").mkdir(parents=True, exist_ok=True)