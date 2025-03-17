import asyncio
import aiohttp
import requests
import os
import time
import hmac
import hashlib
import pyodbc
import csv
import schedule
from datetime import datetime, timezone
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv('1api_info.env')

# Configuration variables
IDENTIFIER = os.getenv('IDENTIFIER')
SECRET_KEY = os.getenv('SECRET')
API_ENDPOINT = "https://tq.cxalloy.com/api/v1"

# SQL Server connection settings
SQL_SERVER_CONFIG = {
    "server": os.getenv('SNAME'),
    "database": os.getenv('DBASE'),
    "username": os.getenv('UNAME'),
    "password": os.getenv('PASS')
}

if not IDENTIFIER or not SECRET_KEY or not SQL_SERVER_CONFIG["server"] or not SQL_SERVER_CONFIG["database"]:
    raise ValueError("Missing environment variables")

DEFAULT_API_TIMEOUT = 200
DEFAULT_DB_TIMEOUT = 200
BATCH_SIZE = 1000
# --- CSV Column Definitions ---
CSV_COLUMNS = [
    "timestamp", "action", "system_id",
    "old_project_id", "new_project_id",
    "old_name", "new_name",
    "old_description", "new_description",
    "old_building_id", "new_building_id",
    "old_building", "new_building",
    "old_discipline_id", "new_discipline_id",
    "old_discipline", "new_discipline",
    "old_timeUpdated", "new_timeUpdated",
    "old_checklist_id", "new_checklist_id",
    "old_checklist_name", "new_checklist_name",
    "old_handover_date", "new_handover_date"
]


def connect_db(timeout=DEFAULT_DB_TIMEOUT):
    try:
        conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={SQL_SERVER_CONFIG['server']};"
                    f"DATABASE={SQL_SERVER_CONFIG['database']};"
                    f"UID={SQL_SERVER_CONFIG['username']};"
                    f"PWD={SQL_SERVER_CONFIG['password']}")
        connection = pyodbc.connect(conn_str, timeout=timeout)
        print("Connected to database.")
        return connection
    except pyodbc.Error as e:
        print(f"Database connection error: {e}")
        raise

def create_table_if_not_exists(connection, table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (table_name,))
        if cursor.fetchone()[0] == 0:
            print(f"Creating table: {table_name}")
            create_table_query = f"""
                CREATE TABLE {table_name} (
                    IDX INT IDENTITY(1,1) PRIMARY KEY,
                    system_id NVARCHAR(255),
                    project_id NVARCHAR(255),
                    name NVARCHAR(255),
                    description NVARCHAR(MAX),
                    building_id NVARCHAR(255),
                    building NVARCHAR(255),
                    discipline_id NVARCHAR(255),
                    discipline NVARCHAR(255),
                    timeUpdated DATETIME,
                    checklist_id NVARCHAR(255),
                    checklist_name NVARCHAR(255),
                    handover_date DATETIME
                )
            """
            cursor.execute(create_table_query)
            connection.commit()
            print(f"Table {table_name} created.")
        else:
            print(f"Table {table_name} already exists.")
    except pyodbc.Error as e:
        print(f"Error creating table {table_name}: {e}")
        raise

def create_history_table_if_not_exists(connection, history_table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (history_table_name,))
        if cursor.fetchone()[0] == 0:
            print(f"Creating history table: {history_table_name}")
            create_table_query = f"""
                CREATE TABLE {history_table_name} (
                    IDX INT IDENTITY(1,1) PRIMARY KEY,
                    system_id NVARCHAR(255),
                    project_id NVARCHAR(255),
                    name NVARCHAR(255),
                    description NVARCHAR(MAX),
                    building_id NVARCHAR(255),
                    building NVARCHAR(255),
                    discipline_id NVARCHAR(255),
                    discipline NVARCHAR(255),
                    timeUpdated DATETIME,
                    checklist_id NVARCHAR(255),
                    checklist_name NVARCHAR(255),
                    handover_date DATETIME,
                    archived_at DATETIME DEFAULT GETDATE()
                )
            """
            cursor.execute(create_table_query)
            connection.commit()
            print(f"History table {history_table_name} created.")
        else:
            print(f"History table {history_table_name} already exists.")
    except pyodbc.Error as e:
        print(f"Error creating history table {history_table_name}: {e}")
        raise


def upsert_data(connection, data, main_table, history_table):
    cursor = connection.cursor()
    rows_added = 0
    rows_updated = 0
    historical_count = 0
    changes_log = []

    for record in tqdm(data, desc="Processing records"):
        system_id = record.get('system_id')
        try:
            if system_id is None:
                insert_query = f"""
                    INSERT INTO {main_table} (system_id, project_id, name, description, building_id, building,
                                             discipline_id, discipline, timeUpdated, checklist_id, checklist_name, handover_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_query, (
                    None, record.get('project_id'), record.get('name'), record.get('description'),
                    record.get('building_id'), record.get('building'), record.get('discipline_id'), record.get('discipline'),
                    record.get('timeUpdated'), record.get('checklist_id'), record.get('checklist_name'),
                    record.get('handover_date')
                ))
                rows_added += 1
                changes_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "action": "insert",
                    "system_id": "None",
                    **{col: "" for col in CSV_COLUMNS if col not in ["timestamp", "action", "system_id"]}
                })
            else:
                select_query = f"""
                    SELECT system_id, project_id, name, description, building_id, building,
                           discipline_id, discipline, timeUpdated, checklist_id, checklist_name, handover_date
                    FROM {main_table}
                    WHERE system_id = ?
                """
                cursor.execute(select_query, (system_id,))
                existing_row = cursor.fetchone()

                if existing_row:
                    existing_record = {
                        "system_id": existing_row[0],
                        "project_id": existing_row[1],
                        "name": existing_row[2],
                        "description": existing_row[3],
                        "building_id": existing_row[4],
                        "building": existing_row[5],
                        "discipline_id": existing_row[6],
                        "discipline": existing_row[7],
                        "timeUpdated": existing_row[8],
                        "checklist_id": existing_row[9],
                        "checklist_name": existing_row[10],
                        "handover_date": existing_row[11]
                    }
                    record_changed = False
                    change_details = {"timestamp": datetime.now().isoformat(), "action": "update", "system_id": system_id}

                    for key in ["project_id", "name", "description", "building_id", "building",
                                "discipline_id", "discipline", "checklist_id", "checklist_name"]:
                        new_val = record.get(key)
                        old_val = existing_record.get(key)
                        if new_val != old_val:
                            record_changed = True
                            change_details[f"old_{key}"] = str(old_val) if old_val is not None else ""
                            change_details[f"new_{key}"] = str(new_val) if new_val is not None else ""
                        else:
                            change_details[f"old_{key}"] = str(old_val) if old_val is not None else ""
                            change_details[f"new_{key}"] = str(new_val) if new_val is not None else ""

                    # Handle datetime separately
                    new_handover = record.get('handover_date')
                    old_handover = existing_record['handover_date']

                    # Add try-except blocks for isoformat conversions
                    try:
                        change_details["old_handover_date"] = old_handover.isoformat() if old_handover else ""
                    except AttributeError:
                        change_details["old_handover_date"] = str(old_handover) if old_handover else ""
                    try:
                        change_details["new_handover_date"] = new_handover.isoformat() if new_handover else ""
                    except AttributeError:
                        change_details["new_handover_date"] = str(new_handover) if new_handover else ""

                    # Handle timeUpdated separately
                    old_time_updated = existing_record.get("timeUpdated")
                    new_time_updated = record.get("timeUpdated")

                    try:
                        change_details["old_timeUpdated"] = old_time_updated.isoformat() if old_time_updated else ""
                    except AttributeError:
                        change_details["old_timeUpdated"] = str(old_time_updated) if old_time_updated else ""
                    try:
                        change_details["new_timeUpdated"] = new_time_updated.isoformat() if new_time_updated else ""
                    except AttributeError:
                        change_details["new_timeUpdated"] = str(new_time_updated) if new_time_updated else ""

                    if record_changed:
                        insert_history_query = f"""
                            INSERT INTO {history_table} (system_id, project_id, name, description, building_id, building,
                                                        discipline_id, discipline, timeUpdated, checklist_id, checklist_name, handover_date)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        cursor.execute(insert_history_query, (
                            existing_record['system_id'], existing_record['project_id'], existing_record['name'],
                            existing_record['description'], existing_record['building_id'], existing_record['building'],
                            existing_record['discipline_id'], existing_record['discipline'], existing_record['timeUpdated'],
                            existing_record['checklist_id'], existing_record['checklist_name'],
                            existing_record['handover_date']
                        ))
                        historical_count += 1

                        update_query = f"""
                            UPDATE {main_table}
                            SET project_id = ?, name = ?, description = ?, building_id = ?, building = ?,
                                discipline_id = ?, discipline = ?, timeUpdated = ?, checklist_id = ?,
                                checklist_name = ?, handover_date = ?
                            WHERE system_id = ?
                        """
                        cursor.execute(update_query, (
                            record.get('project_id'), record.get('name'), record.get('description'),
                            record.get('building_id'), record.get('building'), record.get('discipline_id'),
                            record.get('discipline'), record.get('timeUpdated'), record.get('checklist_id'),
                            record.get('checklist_name'), record.get('handover_date'), system_id
                        ))
                        rows_updated += 1
                        changes_log.append(change_details)

                else:
                    insert_query = f"""
                        INSERT INTO {main_table} (system_id, project_id, name, description, building_id, building,
                                                 discipline_id, discipline, timeUpdated, checklist_id, checklist_name, handover_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    cursor.execute(insert_query, (
                        system_id, record.get('project_id'), record.get('name'), record.get('description'),
                        record.get('building_id'), record.get('building'), record.get('discipline_id'), record.get('discipline'),
                        record.get('timeUpdated'), record.get('checklist_id'), record.get('checklist_name'),
                        record.get('handover_date')
                    ))
                    rows_added += 1
                    changes_log.append({
                        "timestamp": datetime.now().isoformat(),
                        "action": "insert",
                        "system_id": system_id,
                        **{col: "" for col in CSV_COLUMNS if col not in ["timestamp", "action", "system_id"]}
                    })

        except Exception as e:
            print(f"Error processing record with system_id {system_id}: {e}")
            continue
        connection.commit()

    print(f"Processed {len(data)} records. Added: {rows_added}, Updated: {rows_updated}, Archived: {historical_count}")
    changes_log_file = "data_changes_log.csv"
    try:
        with open(changes_log_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(changes_log)
        print(f"Changes logged to {changes_log_file}")
    except IOError as e:
        print(f"Error writing to log file: {e}")
        changes_log_file = None
    return rows_updated, rows_added, historical_count, changes_log_file

def generate_signature(secret, string_to_sign):
    return hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

def get_project_list():
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "cxalloy-identifier": IDENTIFIER,
        "cxalloy-timestamp": timestamp,
        "cxalloy-signature": generate_signature(SECRET_KEY, timestamp)
    }
    try:
        response = requests.get(f"{API_ENDPOINT}/project", headers=headers, timeout=DEFAULT_API_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching project list: {e}")
        return []

def process_equipment_data(data):
    processed_data = []
    for record in data:
        equipment = {
            "system_id": None,
            "project_id": record.get("project_id"),
            "name": record.get("name"),
            "description": record.get("description"),
            "building_id": record.get("building_id"),
            "building": record.get("building"),
            "discipline_id": record.get("discipline_id"),
            "discipline": record.get("discipline"),
            "timeUpdated": datetime.now(timezone.utc),
            "checklist_id": None,
            "checklist_name": record.get("checklist_name"),
            "handover_date": None,
        }
        systems = record.get("systems", [])
        for system in systems:
            equipment["system_id"] = system.get("system_id")
        if not equipment["system_id"]:
            equipment["system_id"] = record.get("system_id")

        attributes = record.get("attributes", [])
        if attributes:
            for attr in attributes:
                if attr.get('name', '').lower() == "handover date":
                    date_str = attr.get('value')
                    if date_str:
                        try:
                            equipment["handover_date"] = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
                        except ValueError:
                            print(f"Invalid handover date format: {date_str}")
                            equipment["handover_date"] = None
                        break

        checklists = record.get("checklists", [])
        for checklist in checklists:
            equipment["checklist_id"] = checklist.get("checklist_id")
            equipment["checklist_name"] = checklist.get("name")
        processed_data.append(equipment)
    return processed_data

async def fetch_data_for_projects(project_ids, includes=None, timeout=DEFAULT_API_TIMEOUT):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
        all_data = []
        for project_id in project_ids:
            print(f"Fetching data for project {project_id}")
            url = f"{API_ENDPOINT}/system"
            params = {"project_id": project_id, "include": ",".join(includes) if includes else ""}
            timestamp = str(int(time.time()))
            headers = {
                "Content-Type": "application/json",
                "cxalloy-identifier": IDENTIFIER,
                "cxalloy-timestamp": timestamp,
                "cxalloy-signature": generate_signature(SECRET_KEY, timestamp)
            }
            page = 1
            while True:
                params["page"] = page
                try:
                    async with session.get(url, params=params, headers=headers) as response:
                        response.raise_for_status()
                        page_data = await response.json()
                        if not page_data:
                            break
                        processed_data = process_equipment_data(page_data)
                        all_data.extend(processed_data)
                        print(f"Fetched {len(page_data)} records for project {project_id}, page {page}")
                        if len(page_data) < 500:
                            break
                        page += 1
                except aiohttp.ClientError as e:
                    print(f"Error fetching data for project {project_id}: {e}")
                    break
        return all_data

def main(timeout_api=DEFAULT_API_TIMEOUT, timeout_db=DEFAULT_DB_TIMEOUT):
    start_time = time.time()
    try:
        projects = get_project_list()
        if not projects:
            print("No projects found.")
            return
        project_ids = [proj['project_id'] for proj in projects if 'GFTX' in proj['name']]
        includes = ["systems", "checklists", "issues"]
        system_data = asyncio.run(fetch_data_for_projects(project_ids, includes=includes, timeout=timeout_api))
        print(f"Fetched {len(system_data)} records.")

        if system_data:
            connection = connect_db(timeout=timeout_db)
            main_table = "GFTX_CXALLOY_System_DATA"
            history_table = "GFTX_CXALLOY_System_DATA_History"
            create_table_if_not_exists(connection, main_table)
            create_history_table_if_not_exists(connection, history_table)
            rows_updated, rows_added, historical_count, _ = upsert_data(connection, system_data, main_table, history_table)
            print(f"Upsert complete. Updated: {rows_updated}, Added: {rows_added}, Archived: {historical_count}")
            connection.close()
        else:
            print("No system data to process.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        end_time = time.time()
        print(f"Total execution time: {end_time - start_time:.2f} seconds")

def job():
    main(timeout_api=500, timeout_db=500)


# Scheduling: Run the main() function at specific times every day
if __name__ == "__main__":
    main()