
import pymssql
import subprocess
import os
from pyhocon import ConfigFactory
import logging as logger
import pandas as pd 
import numpy as np
from datetime import datetime
import sys
import os 
import shutil
from decorators.timing import get_time
import re
import smtplib
from urllib.parse import quote_plus, unquote
from sqlalchemy import create_engine, text
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from decorators.decrypt import decrypt_params
from dotenv import load_dotenv
import pyodbc
import subprocess
import urllib
import unicodedata
import csv

load_dotenv()

# Base functions

def diff_time(start, end):
    diff = end - start
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return (hours, minutes, seconds)
      
def normalize_column_names(df):
    """
    Normalize column names by removing accents and converting to lowercase.
    """
    accented_characters = {'é': 'e', 'ô': 'o', 'à': 'a', 'û': 'u', 'î': 'i'}
    normalized_columns = []
    for col in df.columns:
        normalized_col = ''
        for char in col:
            normalized_char = accented_characters.get(char, char)
            normalized_col += normalized_char
        normalized_col = normalized_col.replace(' ', '_')
        normalized_col = normalized_col.replace("'", '_')
        normalized_columns.append(normalized_col.lower())
    return normalized_columns

def udf_normalize_value(s):
    # To lowercase and trim
    lower_s = s.strip().lower()
    
    # Remove the following chars ! " # % & ' ( ) * + , : ; < = > ? [ ] ^ {}~]", '', lower_s)
    
    # Replace multiple spaces with a single space
    trailing_removed = re.sub(r'\s+', ' ', chars_removed)
    
    # Remove accents
    a_rem = re.sub(r'[àáâãäå]', 'a', trailing_removed)
    e_rem = re.sub(r'[èéêë]', 'e', a_rem)
    i_rem = re.sub(r'[ìíîï]', 'i', e_rem)
    o_rem = re.sub(r'[òóôõö]', 'o', i_rem)
    u_rem = re.sub(r'[ùúûü]', 'u', o_rem)
    
    return u_rem.upper()

def process_combined_dataframes(dataframes: list, directory: str) -> pd.DataFrame:
    """
    Processes and concatenates DataFrames with common transformations.

    Args:
        dataframes (list): List of DataFrames to be concatenated and processed.
        directory (str): Directory path for logging.

    Returns:
        pd.DataFrame: A processed and concatenated DataFrame.
    """
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df['load_file'] = pd.to_datetime(combined_df['load_file'])
        combined_df = combined_df.where(pd.notnull(combined_df), None)
        for column in combined_df.columns:
            combined_df[column] = combined_df.apply(udf_normalize_value)
        combined_df = combined_df.fillna('None')
        combined_df.columns = normalize_column_names(combined_df)
        

        logger.info(f"Len of DataFrames found in {directory}: {len(combined_df)}")
        logger.info(f"Number of columns in the compiled DataFrame: {len(combined_df.columns)}")
        return combined_df
    else:
        logger.error("No valid files found in the directory.")
        return pd.DataFrame()



# Personal functions
# Function: build_sql_pymssql



# Function to connect to a SQL Server database using pymssql

@decrypt_params
def build_sql_pymssql(server, database, username, password):
    """
    Connects to a SQL Server database using the pymssql library.

    Parameters:
        server (str): The hostname or IP address of the SQL Server.
        database (str): The name of the database to connect to.
        username (str): The username for database authentication.
        password (str): The password for database authentication.

    Returns:
        tuple: A tuple containing:
            - conn (pymssql.Connection): The connection object to the database.
            - cur (pymssql.Cursor): The cursor object for executing SQL queries.

    Raises:
        pymssql.Error: If an error occurs during the connection attempt.
    """
    try:
        # Create a connection to the SQL Server
        conn = pymssql.connect(
            host=rf'{server}',   # Server address or hostname
            user=rf'{username}', # Username for authentication
            password=rf'{password}', # Password for authentication
            database=rf'{database}'  # Target database
        )

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Log a success message
        logger.info("Successfully connected to the database.")

        # Return the connection and cursor objects
        return conn, cur

    except pymssql.Error as e:
        # Log an error message if the connection fails
        logger.error(f"An error occurred while connecting to the database: {e}")

        # Re-raise the exception to propagate it
        raise e



def build_sql_sqlalchemy_dwh():

    try:
        conn_str = os.getenv("SQL_CONNECTION_STRING_DWH")
        
        engine = create_engine(conn_str, fast_executemany=True)
        logger.info("✅ Successfully connected to the database.")
        return engine
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        raise

def build_sql_sqlalchemy_dwh_dw():

    try:
        conn_str = os.getenv("SQL_CONNECTION_STRING_DWH_DW")
        
        engine = create_engine(conn_str, fast_executemany=True)
        logger.info("✅ Successfully connected to the database.")
        return engine
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        raise

def build_sql_sqlalchemy_bc():

    try:
        conn_str = os.getenv("SQL_CONNECTION_STRING_BC")
        
        engine = create_engine(conn_str, fast_executemany=True)
        logger.info("✅ Successfully connected to the database.")
        return engine
    except Exception as e:
        logger.error(f"❌ Failed to connect: {e}")
        raise


def convert_column_dtype(df: pd.DataFrame, column_name: str, sql_dtype: str):
    """Convert a DataFrame column to match the SQL data type."""
    if sql_dtype.upper() in ["NVARCHAR", "VARCHAR"]:
        return df[column_name].astype(str)
    elif sql_dtype.upper() == "INT":
        return pd.to_numeric(df[column_name], errors='coerce').fillna(0).astype(int)
    elif sql_dtype.upper() == "FLOAT":
        return pd.to_numeric(df[column_name], errors='coerce')
    elif sql_dtype.upper() == "DATETIME":
        return pd.to_datetime(df[column_name], errors='coerce')
    else:
        return df[column_name]


def clean_dataframe(df: pd.DataFrame, sql_column_types: dict) -> pd.DataFrame:
    """
    Nettoie le DataFrame pour :
      - remplacer 'None' par chaîne vide
      - trim gauche/droite des espaces
      - enlever les pipes et retours à la ligne
      - normaliser l’UTF-8
      - tronquer toutes les colonnes texte selon max_length
      - ne garder que digits et point pour les numériques
      - formater les dates
    """
    df = df.copy()
    # 1) Remplace 'None' ➞ ''
    df.replace('None', '', inplace=True)
    
    for col, (sql_dtype, max_len) in sql_column_types.items():
        if col not in df.columns:
            continue
        
        # Cast en str
        df[col] = df[col].astype(str)
        
        # 2) Trim espaces, nettoyage UTF-8 et suppression des pipes/retours
        df[col] = (
            df[col]
            .str.strip()  # supprime espaces début/fin
            .map(lambda s: unicodedata.normalize('NFC', s))
            .str.encode('utf-8', errors='replace')
            .str.decode('utf-8')
            .str.replace(r'\|', '', regex=True)       # retire tous les '|'
            .str.replace(r'[\r\n\t]', ' ', regex=True) # remplace retours/tabs par espace
            .str.strip()                               # re-trim après remplacements
        )
        
        # 3) Tronquage des colonnes texte
        if sql_dtype.lower() in ("char", "varchar", "nchar", "nvarchar") and max_len > 0:
            max_chars = max_len if sql_dtype.lower() in ("char", "varchar") else max_len // 2
            df[col] = df[col].str.slice(0, max_chars)
        
        # 4) Colonnes numériques
        elif sql_dtype.lower() in ("int", "bigint", "smallint", "decimal", "numeric", "float", "real"):
            df[col] = (
                df[col]
                .str.replace(',', '.', regex=False)
                .str.replace(r'[^0-9.\-]', '', regex=True)
            )
        
        # 5) Colonnes date/heure
        elif sql_dtype.lower() in ("date", "datetime", "datetime2", "smalldatetime"):
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df


from prefect import get_run_logger
import re
# Alternative version with XML format (if needed)

def force_utf8_clean(s):
    if isinstance(s, str):
        try:
            return s.encode('cp1252').decode('utf-8')
        except:
            try:
                return s.encode('latin1').decode('utf-8')
            except:
                return s.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
    return s

def verify_and_fix_format_file(fmt_path, expected_columns, list_csv_columns):
    logger = get_run_logger()
    with open(fmt_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Nombre de colonnes (ligne 2)
    actual_columns = int(lines[1].strip())
    logger.info(f"Nombre de colonnes dans le format file: {actual_columns}")
    logger.info(f"Nombre de colonnes dans le CSV: {expected_columns}")

    # Lignes des colonnes
    column_lines = lines[2:2 + actual_columns]

    # Extraire l'avant-dernier champ non vide de chaque ligne (le nom de colonne)
    column_names = [line.strip().split()[-2] for line in column_lines]

    # afficher les colonnes manquantes 
    missing_columns = [col for col in column_names if col not in list_csv_columns]

    if missing_columns:
        logger.error(f"❌ Missing columns in format file: {missing_columns}")

    if actual_columns != expected_columns:
        raise ValueError(f"❌ Format file column count mismatch: {actual_columns} in .fmt vs {expected_columns} in CSV")

    # Vérifie et corrige le terminator de la dernière ligne
    last_line = lines[-1]
    if '"\\n"' in last_line:
        corrected_last_line = last_line.replace('"\\n"', '"\\r\\n"')
        lines[-1] = corrected_last_line
        with open(fmt_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        logger.info(f"✅ Corrected line terminator in format file: '\\n' → '\\r\\n'")
    else:
        logger.info(f"✅ Format file already has correct line terminator.")



def insert_df_bcp(
    df: pd.DataFrame,
    table_name_with_schema: str,
    batch_size: int = 10000
):
    logger = get_run_logger()
    temp_csv = None
    fmt_path = None
    try:
        schema, table = table_name_with_schema.split('.')
        engine = build_sql_sqlalchemy_dwh()

        with engine.connect() as connection:
            # Fetch column information
            sql_query = f"""
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    c.max_length
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE object_id = OBJECT_ID('{table_name_with_schema}')
                ORDER BY column_id;
            """
            result = connection.execute(text(sql_query))
            sql_columns = result.fetchall()

            if not sql_columns:
                raise ValueError(f"Table '{table_name_with_schema}' not found or no columns retrieved")

            # Create dictionaries for column types and lengths
            sql_column_types = {col.column_name: (col.data_type, col.max_length) for col in sql_columns}
            matched_columns = [col.column_name for col in sql_columns if col.column_name in df.columns]
            if not matched_columns:
                raise ValueError(f"No matching columns found between DataFrame and table '{table_name_with_schema}'")

            df_to_insert = df[matched_columns].copy()
            df_to_insert = clean_dataframe(df_to_insert, sql_column_types)
            # Process each column
            for col in matched_columns:
                sql_dtype, max_len = sql_column_types[col]
                logger.info(f"Processing column '{col}' with SQL type '{sql_dtype}' and max_length {max_len}")
                df_to_insert[col] = df_to_insert[col].astype(str)
                if sql_dtype in ("nvarchar", "varchar") and max_len > 0:
                    max_chars = max_len if sql_dtype == "varchar" else max_len // 2
                    df_to_insert[col] = df_to_insert[col].str.slice(0, max_chars)
                    logger.info(f"✅ Truncated column '{col}' to {max_chars} characters")
                # Clean unwanted characters
                df_to_insert[col] = (
                    df_to_insert[col]
                    .str.replace('|', '', regex=False)
                    .str.replace('\n', ' ', regex=False)
                    .str.replace('\r', ' ', regex=False)
                    .str.replace('\t', ' ', regex=False)
                )
            
            df_to_insert = df_to_insert.replace('None', '', regex=False)

            # Prepare temp file paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_csv = f'/tmp/{table}_{timestamp}.csv'
            fmt_path = f'/tmp/{table}_{timestamp}.fmt'

            df_to_insert = df_to_insert.fillna('')
            # df_to_insert = df_to_insert.applymap(force_utf8_clean)
            df_to_insert.to_csv(
                temp_csv,
                index=False,
                sep='\t',
                header=False,
                encoding='utf-8-sig',
                lineterminator='\r\n',
                quoting=csv.QUOTE_NONE,
                escapechar='\\'
            )
            logger.info(f"CSV file created at {temp_csv} ({os.path.getsize(temp_csv)} bytes, {len(df_to_insert)} rows)")

            # log csv columns
            logger.info(f"CSV columns ({len(df_to_insert.columns)}): {list(df_to_insert.columns)}")

            # Config
            conf = ConfigFactory.parse_file("src/config.conf")
            # Test BCP connection using a SELECT query for queryout
            bcp_path = "/opt/mssql-tools18/bin/bcp"
            server = conf.get('db_dwh.server')
            username = conf.get('db_dwh.username')
            password = conf.get('db_dwh.password')
            database_name = conf.get('db_dwh.database_ods')
            test_cmd = [
                bcp_path,
                f"SELECT TOP 1 * FROM dbo.DimCustomer",
                "queryout",
                "/tmp/test_connection.txt",
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", "DW_NOVUS",
                "-U", username,
                "-P", password,
                "-c",
                "-Yo"
            ]
            logger.info("Testing database connection via SELECT TOP 1...")
            subprocess.run(test_cmd, check=True, capture_output=True, text=True, timeout=30)
            if os.path.exists("/tmp/test_connection.txt"):
                os.remove("/tmp/test_connection.txt")
            logger.info("✅ Database connection test successful")

            # Generate BCP format file

            format_cmd = [
                bcp_path,
                table_name_with_schema,
                "format",
                "/dev/null",
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", database_name,
                "-U", username,
                "-P", password,
                "-c",              # character format
                "-t", "\t",        # tab separator
                "-f", fmt_path,
                "-Yo"
            ]
            logger.info(f"Generating BCP format file at {fmt_path}")
            subprocess.run(format_cmd, check=True, capture_output=True, text=True, timeout=60, encoding='cp1252')
            logger.info("✅ Format file generated successfully")
            
            # Vérifier et corriger le nombre de colonnes dans le fichier format
            verify_and_fix_format_file(fmt_path, len(df_to_insert.columns), list(df_to_insert.columns))

            # Construct and run BCP import
            error_log = f'/tmp/bcp_error_{timestamp}.log'
            bcp_cmd = [
                bcp_path,
                table_name_with_schema,
                "in",
                temp_csv,
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", database_name,
                "-U", username,
                "-P", password,
                "-f", fmt_path,
                "-e", error_log,
                "-Yo",
                "-v"
                
            ]
            logger.info(f"Starting BCP import for {len(df_to_insert)} rows")
            result = subprocess.run(bcp_cmd, check=True, capture_output=True, text=True, timeout=300)
            logger.info("✅ BCP import completed successfully")
            if os.path.exists(error_log):
                with open(error_log, 'r', encoding='cp1252', errors='replace') as ef:
                    output = ef.read().strip()
                    if output:
                        logger.info(f"BCP output log:\n{output}")
    except Exception as e:
        logger.error(f"Failed to insert data into '{table_name_with_schema}': {e}", exc_info=True)
        logger.error("BCP stderr:\n%s", result.stderr)
        raise
    finally:
        # Nettoyer les fichiers temporaires si nécessaire
        for tmp in (temp_csv, fmt_path):
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                    logger.info(f"Cleaned up temporary file: {tmp}")
                except Exception as cleanup_err:
                    logger.warning(f"Failed to clean up {tmp}: {cleanup_err}")




def insert_df_bcp_dw(
    df: pd.DataFrame,
    table_name_with_schema: str,
    batch_size: int = 10000
):
    logger = get_run_logger()
    temp_csv = None
    fmt_path = None
    try:
        schema, table = table_name_with_schema.split('.')
        engine = build_sql_sqlalchemy_dwh()

        with engine.connect() as connection:
            # Fetch column information
            sql_query = f"""
                SELECT 
                    c.name as column_name,
                    t.name as data_type,
                    c.max_length
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE object_id = OBJECT_ID('{table_name_with_schema}')
                ORDER BY column_id;
            """
            result = connection.execute(text(sql_query))
            sql_columns = result.fetchall()

            if not sql_columns:
                raise ValueError(f"Table '{table_name_with_schema}' not found or no columns retrieved")

            # Create dictionaries for column types and lengths
            sql_column_types = {col.column_name: (col.data_type, col.max_length) for col in sql_columns}
            matched_columns = [col.column_name for col in sql_columns if col.column_name in df.columns]
            if not matched_columns:
                raise ValueError(f"No matching columns found between DataFrame and table '{table_name_with_schema}'")

            df_to_insert = df[matched_columns].copy()
            df_to_insert = clean_dataframe(df_to_insert, sql_column_types)
            # Process each column
            for col in matched_columns:
                sql_dtype, max_len = sql_column_types[col]
                logger.info(f"Processing column '{col}' with SQL type '{sql_dtype}' and max_length {max_len}")
                df_to_insert[col] = df_to_insert[col].astype(str)
                if sql_dtype in ("nvarchar", "varchar") and max_len > 0:
                    max_chars = max_len if sql_dtype == "varchar" else max_len // 2
                    df_to_insert[col] = df_to_insert[col].str.slice(0, max_chars)
                    logger.info(f"✅ Truncated column '{col}' to {max_chars} characters")
                # Clean unwanted characters
                df_to_insert[col] = (
                    df_to_insert[col]
                    .str.replace('|', '', regex=False)
                    .str.replace('\n', ' ', regex=False)
                    .str.replace('\r', ' ', regex=False)
                    .str.replace('\t', ' ', regex=False)
                )
            
            df_to_insert = df_to_insert.replace('None', '', regex=False)

            # Prepare temp file paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_csv = f'/tmp/{table}_{timestamp}.csv'
            fmt_path = f'/tmp/{table}_{timestamp}.fmt'

            df_to_insert = df_to_insert.fillna('')
            # df_to_insert = df_to_insert.applymap(force_utf8_clean)
            df_to_insert.to_csv(
                temp_csv,
                index=False,
                sep='\t',
                header=False,
                encoding='utf-8-sig',
                lineterminator='\r\n',
                quoting=csv.QUOTE_NONE,
                escapechar='\\'
            )
            logger.info(f"CSV file created at {temp_csv} ({os.path.getsize(temp_csv)} bytes, {len(df_to_insert)} rows)")

            # log csv columns
            logger.info(f"CSV columns ({len(df_to_insert.columns)}): {list(df_to_insert.columns)}")


            conf = ConfigFactory.parse_file("src/config.conf")
            # Test BCP connection using a SELECT query for queryout
            bcp_path = "/opt/mssql-tools18/bin/bcp"
            server = conf.get('db_dwh.server')
            username = conf.get('db_dwh.username')
            password = conf.get('db_dwh.password')
            database_name = conf.get('db_dwh.database_dwh')
            test_cmd = [
                bcp_path,
                f"SELECT TOP 1 * FROM dbo.DimCustomer",
                "queryout",
                "/tmp/test_connection.txt",
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", "DW_NOVUS",
                "-U", username,
                "-P", password,
                "-c",
                "-Yo"
            ]
            logger.info("Testing database connection via SELECT TOP 1...")
            subprocess.run(test_cmd, check=True, capture_output=True, text=True, timeout=30)
            if os.path.exists("/tmp/test_connection.txt"):
                os.remove("/tmp/test_connection.txt")
            logger.info("✅ Database connection test successful")

            # Generate BCP format file

            format_cmd = [
                bcp_path,
                table_name_with_schema,
                "format",
                "/dev/null",
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", database_name,
                "-U", username,
                "-P", password,
                "-c",              # character format
                "-t", "\t",        # tab separator
                "-f", fmt_path,
                "-Yo"
            ]
            logger.info(f"Generating BCP format file at {fmt_path}")
            subprocess.run(format_cmd, check=True, capture_output=True, text=True, timeout=60, encoding='cp1252')
            logger.info("✅ Format file generated successfully")
            
            # Vérifier et corriger le nombre de colonnes dans le fichier format
            verify_and_fix_format_file(fmt_path, len(df_to_insert.columns), list(df_to_insert.columns))

            # Construct and run BCP import
            error_log = f'/tmp/bcp_error_{timestamp}.log'
            bcp_cmd = [
                bcp_path,
                table_name_with_schema,
                "in",
                temp_csv,
                "-S", f"tcp:{server}.company_name.lan,1433",
                "-d", database_name,
                "-U", username,
                "-P", password,
                "-f", fmt_path,
                "-e", error_log,
                "-Yo",
                "-v"
                
            ]
            logger.info(f"Starting BCP import for {len(df_to_insert)} rows")
            result = subprocess.run(bcp_cmd, check=True, capture_output=True, text=True, timeout=300)
            logger.info("✅ BCP import completed successfully")
            if os.path.exists(error_log):
                with open(error_log, 'r', encoding='cp1252', errors='replace') as ef:
                    output = ef.read().strip()
                    if output:
                        logger.info(f"BCP output log:\n{output}")
    except Exception as e:
        logger.error(f"Failed to insert data into '{table_name_with_schema}': {e}", exc_info=True)
        logger.error("BCP stderr:\n%s", result.stderr)
        raise
    finally:
        pass
        # Nettoyer les fichiers temporaires si nécessaire
        # for tmp in (temp_csv, fmt_path):
        #     if tmp and os.path.exists(tmp):
        #         try:
        #             os.remove(tmp)
        #             logger.info(f"Cleaned up temporary file: {tmp}")
        #         except Exception as cleanup_err:
        #             logger.warning(f"Failed to clean up {tmp}: {cleanup_err}")