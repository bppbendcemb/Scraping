import pyodbc
from config import db_bbn6, db_home, db_window

# def database(server_choice='bbn6'):
#     # Get the server details based on the chosen database configuration
#     if server_choice == 'bbn6':
#         connection_string = db_bbn6()
#     elif server_choice == 'home':
#         connection_string = db_home()
#     elif server_choice == 'window':
#         connection_string = db_window()
#     else:
#         raise ValueError("Invalid server choice. Choose 'bbn6', 'home', or 'window'.")

#     # Establish and return the database connection
#     try:
#         conn = pyodbc.connect(connection_string)
#         return conn
#     except pyodbc.Error as e:
#         print("Error during database connection:", e)
#         return None


def database():
    # Choose the database configuration
    connection_string = db_bbn6()  # or db_home(), db_window() as needed

    # Create a connection to the database
    conn = pyodbc.connect(connection_string)
    return conn

