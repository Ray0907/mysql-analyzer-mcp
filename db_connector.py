import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

def get_db_connection():
    """
    Establishes a connection to the MySQL database using credentials from .env file.
    
    Returns:
        mysql.connector.connection.MySQLConnection object or None if connection fails.
    """
    load_dotenv()
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_DATABASE')
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None
    return None

def get_db_name():
    """
    Retrieves the database name from the .env file.
    
    Returns:
        str: The database name.
    """
    load_dotenv()
    return os.getenv('DB_DATABASE')

