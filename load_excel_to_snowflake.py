
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import math
import numpy as np

# 1. Excel file and worksheet
EXCEL_FILE = r"C:\Users\DurAkula\Downloads\Train_mvmt_project\data_extraction_files\codes.xlsx"
SHEET_NAME = "Stanox Codes"

# 2. Snowflake destination
DATABASE = "RAIL_DATA"
SCHEMA = "PUBLIC"
TABLE = "STATION_CODES"

# 3. Read only the required worksheet
df = pd.read_excel(
    EXCEL_FILE,
    sheet_name=SHEET_NAME,
    engine="openpyxl"
)

# Clean column names for easier Snowflake usage
df.columns = [
    str(col).strip().upper().replace(" ", "_").replace(".", "")
    for col in df.columns
]

print("Rows:", len(df))
print("Columns:", df.columns.tolist())
print(df.head())

# 4. Connect to Snowflake
conn = snowflake.connector.connect(
    account = 'WISMGCK-TW41160',
    user="HarryPotter",
    password = 'Advvamsi@37870',
    warehouse="COMPUTE_WH",
    database=DATABASE,
    schema=SCHEMA,
    role="ACCOUNTADMIN",
    paramstyle="qmark"
)

try:
    # 5. Load the DataFrame into Snowflake.
    # Convert NaN and NaT values to Python None
    
# Convert missing Excel values (NaN, NaT, pandas NA) to Python None.
# Python None is inserted into Snowflake as SQL NULL.

    def clean_value(value):
        if isinstance(value, np.generic):
            value = value.item()

        if value is None:
            return None

        # Handle NaN, NaT, pd.NA, and other pandas missing values.
        try:
            missing = pd.isna(value)
            if isinstance(missing, (bool, np.bool_)) and missing:
                return None
        except (TypeError, ValueError):
            pass

        # Extra protection against floating-point NaN.
        if isinstance(value, float) and math.isnan(value):
            return None

        return value


    # Clean every cell before building the rows for Snowflake.
    df = df.astype(object)
    rows = [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]

    # Convert STANOX_NO to an integer where it has a value.
    stanox_index = df.columns.get_loc("STANOX_NO")

    # Rebuild rows from the DataFrame, applying cleanup to every cell.
    rows = [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]

    # Convert STANOX_NO to integer, preserving missing values as None.
    stanox_index = df.columns.get_loc("STANOX_NO")
    rows = [
        tuple(
            int(value) if i == stanox_index and value is not None else value
            for i, value in enumerate(row)
        )
        for row in rows
    ]

    print("Rows ready:", len(rows))

    # for row_index, row in enumerate(rows):
    #     for col_index, value in enumerate(row):
    #         if isinstance(value, float) and math.isnan(value):
    #             print(
    #                 "NaN found at Excel data row:",
    #                 row_index + 2,
    #                 "| Column:",
    #                 df.columns[col_index],
    #                 "| Value:",
    #                 repr(value)
    #             )
    sql = """
    INSERT INTO RAIL_DATA.PUBLIC.STATION_CODES
        (STANOX_NO, FULL_NAME, CRS_CODE, ROUTE_DESCRIPTION)
    VALUES (?, ?, ?, ?)
    """

    rows = list(df.itertuples(index=False, name=None))

    with conn.cursor() as cursor:
        batch_size = 500

        for start in range(0, len(rows), batch_size):
            cursor.executemany(sql, rows[start:start + batch_size])

    print(f"Inserted {len(rows)} rows.")

finally:
    #pass
     conn.close()
