import mysql.connector
from mysql.connector import Error
from config import get_config

def get_db_connection():
    """
    Establishes a connection to the MySQL database using credentials from ConfigManager.
    
    Returns:
        mysql.connector.connection.MySQLConnection object or None if connection fails.
    """
    config = get_config()
    db_config = config.get_db_connection_args()
    
    if not all(db_config.values()):
        print("Database configuration is incomplete. Please check your .env file.")
        return None

    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None
    return None
