def db_bbn6():
    server_name = 'BBN6\\SQLEXPRESS'
    database_name = 'KPI'
    user_name = 'bendcemb'
    password = '1234'
    
    # Return the connection string for the BBN6 server
    return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password};"

def db_window():
    server_name = 'c259-003\\SQLEXPRESS'
    database_name = 'KPI'
    
    # Return the connection string for the window server
    return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;"

def db_home():
    server_name = 'bendcemb\\SQLEXPRESS'
    database_name = 'KPI'
    user_name = 'bendcemb'
    password = '1234'
    
    # Return the connection string for the home server
    return f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};UID={user_name};PWD={password};"
