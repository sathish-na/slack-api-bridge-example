from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

def get_db_connection(host, port, database, user, password):
    try:
        # URL encode the password to handle special characters like @
        encoded_password = quote_plus(password)
        
        # Use the encoded password in the connection string
        engine = create_engine(
            f"mysql+pymysql://{user}:{encoded_password}@{host}:{port}/{database}"
        )
        
        # Test the connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")