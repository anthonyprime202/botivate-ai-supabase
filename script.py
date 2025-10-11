import os
import re
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# --- Configuration ---
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
APP_SCRIPT_URL = os.getenv("APPS_SCRIPT_URL")
DB_CONNECTION_STRING = os.getenv("DATABASE_URI")


# ---------- HELPER FUNCTIONS ----------

def detect_date_or_timestamp(value: str):
    """Detect if a string represents a date or timestamp, including ISO 8601."""
    if not isinstance(value, str):
        return None

    value = value.strip()
    if not value:
        return None

    # --- ISO 8601 Detection (with timezone or milliseconds) ---
    iso_pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}"
        r"([ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
    )
    if iso_pattern.match(value):
        # If time part is present -> TIMESTAMP
        if "T" in value or ":" in value:
            return "TIMESTAMP"
        return "DATE"

    # --- Common date formats ---
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]
    for fmt in date_formats:
        try:
            datetime.strptime(value, fmt)
            return "DATE"
        except ValueError:
            pass

    # --- Common datetime formats ---
    datetime_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
    ]
    for fmt in datetime_formats:
        try:
            datetime.strptime(value, fmt)
            return "TIMESTAMP"
        except ValueError:
            pass

    return None


def normalize_datetime_value(value: str, detected_type: str):
    """Convert recognized date/timestamp to ISO 8601 normalized form for PostgreSQL."""
    if not detected_type or not isinstance(value, str):
        return value

    value = value.strip()
    if not value:
        return value

    # Handle ISO-style directly
    iso_match = re.match(
        r"^(\d{4}-\d{2}-\d{2})([ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$", value
    )
    if iso_match:
        if detected_type == "DATE":
            return value[:10]
        else:
            # Ensure T separator for timestamp
            return value.replace(" ", "T")

    # Try standard formats
    formats = {
        "DATE": ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"],
        "TIMESTAMP": [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%m/%d/%Y %H:%M",
        ],
    }

    for fmt in formats.get(detected_type, []):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.isoformat(timespec="seconds")
        except ValueError:
            pass

    return value


def get_value_type_level(value):
    """Determines the type level: 0=BIGINT, 1=FLOAT8, 2=DATE, 3=TIMESTAMP, 4=TEXT"""
    if value is None or str(value).strip() == "":
        return -1

    # Try numeric first
    try:
        if float(str(value)) == int(float(str(value))):
            return 0  # BIGINT
    except (ValueError, TypeError):
        pass

    try:
        float(str(value))
        return 1  # FLOAT8
    except (ValueError, TypeError):
        pass

    # Try date/timestamp detection
    detected = detect_date_or_timestamp(str(value))
    if detected == "DATE":
        return 2
    elif detected == "TIMESTAMP":
        return 3

    return 4  # TEXT fallback


def get_column_types(rows):
    """Scan rows and determine final SQL type per column."""
    if not rows:
        return {}
    headers = list(rows[0].keys())
    type_hierarchy = ["BIGINT", "FLOAT8", "DATE", "TIMESTAMP", "TEXT"]
    column_levels = {h: 0 for h in headers}

    for row in rows:
        for col_name, value in row.items():
            if col_name not in column_levels:
                continue
            value_level = get_value_type_level(value)
            if value_level > column_levels[col_name]:
                column_levels[col_name] = value_level

    return {h: type_hierarchy[level] for h, level in column_levels.items()}


# ---------- MAIN SYNC LOGIC ----------

def sync_to_db():
    """Fetch data from Apps Script and sync to PostgreSQL (Supabase)."""
    if not all([SUPABASE_URL, SUPABASE_KEY, APP_SCRIPT_URL, "[YOUR-PASSWORD]" not in DB_CONNECTION_STRING]):
        print("❌ Error: Missing environment variables or placeholder password.")
        return

    try:
        print("🚀 Fetching data from Google Apps Script...")
        response = requests.get(APP_SCRIPT_URL)
        response.raise_for_status()
        sheets_data = response.json()
        print(f"✅ Fetched data for {len(sheets_data)} sheet(s).")

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
    except Exception as e:
        print(f"❌ Setup error: {e}")
        return

    for table_name, rows in sheets_data.items():
        sanitized_table_name = "".join(c if c.isalnum() else '_' for c in table_name)
        print(f"\n--- Processing table: {sanitized_table_name} ---")

        if not rows:
            print("- Skipping empty sheet.")
            continue

        column_definitions = get_column_types(rows)

        try:
            cursor.execute(f'DROP TABLE IF EXISTS "{sanitized_table_name}";')
            cols_sql = ", ".join(
                f'"{"".join(c if c.isalnum() else "_" for c in col)}" {sql_type}'
                for col, sql_type in column_definitions.items()
            )
            create_table_sql = f'CREATE TABLE "{sanitized_table_name}" (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {cols_sql});'
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.execute("NOTIFY pgrst, 'reload schema';")
            print(f"✅ Table '{sanitized_table_name}' created.")
        except Exception as e:
            print(f"❌ Schema error for '{sanitized_table_name}': {e}")
            conn.rollback()
            continue

        try:
            clean_rows = []
            for row in rows:
                clean_row = {}
                for key, val in row.items():
                    if not key:
                        continue
                    sanitized_key = "".join(c if c.isalnum() else '_' for c in key)
                    if val is None or str(val).strip() == "":
                        clean_row[sanitized_key] = None
                        continue
                    # Normalize date/timestamp
                    detected = detect_date_or_timestamp(str(val))
                    if detected:
                        val = normalize_datetime_value(str(val), detected)
                    clean_row[sanitized_key] = val
                clean_rows.append(clean_row)

            print(f"- Inserting {len(clean_rows)} rows...")
            supabase.table(sanitized_table_name).upsert(clean_rows).execute()
            print(f"✅ Inserted data for '{sanitized_table_name}'.")
        except Exception as e:
            print(f"❌ Insert error for '{sanitized_table_name}': {e}")
            continue

    cursor.close()
    conn.close()
    print("\n🎉 Sync complete!")


if __name__ == "__main__":
    sync_to_db()
