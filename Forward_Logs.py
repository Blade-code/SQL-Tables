import pyodbc
import logging
import logging.handlers
import socket
import os
import json
from getpass import getpass  # For securely getting password input
from colorama import Fore, Style  # For colored output

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Paths to the configuration files in the same directory as the script
SERVER_CONFIG_FILE = os.path.join(current_dir, 'Login.json')
TABLES_CONFIG_FILE = os.path.join(current_dir, 'Tables.txt')

# Syslog server details
SYSLOG_SVR_IP = "ssylog ip"
SYSLOG_SVR_PORT = 514  # default syslog port

# State file path in the same directory as the script
STATE_FILE = os.path.join(current_dir, 'SQL.state')

# Function to load server configurations from a JSON file
def load_server_configs():
    with open(SERVER_CONFIG_FILE, 'r') as f:
        return json.load(f)

# Function to load table configurations from the tables.txt file
def load_table_configs():
    table_configs = []
    server_configs = load_server_configs()
    server_dict = {config['server_ip']: config for config in server_configs}

    with open(TABLES_CONFIG_FILE, 'r') as f:
        for line in f:
            server_ip, db_name, table_name = line.strip().split(':')
            if server_ip in server_dict:
                config = server_dict[server_ip]
                table_configs.append({
                    'server_ip': config['server_ip'],
                    'user': config['user'],
                    'database': db_name,
                    'table': table_name
                })
            else:
                print(f"{Fore.RED}Cannot find '{server_ip}' in the server configurations.{Style.RESET_ALL}")
    return table_configs

# Function to create a state file if it does not exist
def create_state_file(table_configs):
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as f:
            for config in table_configs:
                f.write(f"{config['table']}:0\n")
        print("State file created!")

# Function to read the current state from the file
def read_state():
    state = {}
    if (os.path.exists(STATE_FILE)):
        with open(STATE_FILE, "r") as f:
            for line in f:
                parts = line.strip().split(":")
                if len(parts) == 2:
                    table_name, last_rows_read = parts
                    state[table_name] = int(last_rows_read)
    return state

# Function to update the state file with the last rows read
def update_state(table_name, last_rows_read):
    state = read_state()
    state[table_name] = last_rows_read
    with open(STATE_FILE, "w") as f:
        for table, rows in state.items():
            f.write(f"{table}:{rows}\n")

# Function to fetch data from the database for a given configuration
def fetch_data_from_db(config):
    try:
        state = read_state()
        last_processed_id = state.get(config['table'], 0)

        password = getpass(f"Enter password for {config['user']}@{config['server_ip']}: ")

        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={config['server_ip']};"
            f"DATABASE={config['database']};"
            f"UID={config['user']};"
            f"PWD={password};"
            f"TrustServerCertificate=yes"
        )
        cursor = connection.cursor()
        
        cursor.execute(f"SELECT * FROM {config['table']}")
        rows = cursor.fetchall()
        connection.close()
        
        return rows
    except pyodbc.Error as e:
        print("Error fetching data:", e)
        return []
    
# Function to ping the syslog server to check connectivity
def ping_syslog_server():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((SYSLOG_SVR_IP, SYSLOG_SVR_PORT))
        sock.close()
        return result == 0  # Return True if connection successful
    except Exception as e:
        print(f"Error pinging syslog server: {e}")
        return False

# Function to send data to the syslog server
def send_to_syslog(data, config):
    logger = logging.getLogger('SyslogLogger')
    logger.setLevel(logging.INFO)
    syslog_handler = logging.handlers.SysLogHandler(address=(SYSLOG_SVR_IP, SYSLOG_SVR_PORT), ssl=False)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    syslog_handler.setFormatter(formatter)
    logger.addHandler(syslog_handler)

    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)

    state = read_state()
    last_rows_read = state.get(config['table'], 0)

    if len(data) > last_rows_read:
        new_data = data[last_rows_read:]
        row_count = len(new_data)
        
        message = f"{hostname} ({ip_address}) [{config['server_ip']}] | {config['database']} | {config['table']} | Rows Read: {row_count}"
        logger.info(message)
        print(f"{Fore.GREEN}{message}{Style.RESET_ALL}")
        
        for row in new_data:
            row_message = f"{hostname} ({ip_address}) [{config['server_ip']}] | {config['database']} | {config['table']} | " + ' | '.join(map(str, row))
            logger.info(row_message)
            print(f"{Fore.GREEN}{row_message}{Style.RESET_ALL}")
        
        update_state(config['table'], last_rows_read + row_count)
        print(f"{Fore.GREEN}{row_count} new rows logged for {config['table']} from {config['server_ip']}.{Style.RESET_ALL}")
    else:
        print(f"No new rows to log for {config['table']} from {config['server_ip']}.")

# Main function
def main():
    server_configs = load_server_configs()
    table_configs = load_table_configs()
    create_state_file(table_configs)
    
    # Ping syslog server before sending logs
    if not ping_syslog_server():
        print(f"{Fore.RED}Cannot contact the Syslog server. Ensure the Syslog server is on.{Style.RESET_ALL}")
        return
    
    for config in table_configs:
        data = fetch_data_from_db(config)
        
        if data:
            send_to_syslog(data, config)

if __name__ == "__main__":
    main()
