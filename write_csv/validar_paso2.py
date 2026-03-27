
from src.extract.oracle_reader import OracleReader
from src.load.excel_writer import save_triad_excel
from src.transform.profiler import segregate_data


reader = OracleReader()
try:
    print("Extracting PACIENTES for triad test...")
    df = reader.extract_table_paginated("PACIENTES", batch_size=500)

    # Apply segregation logic
    df_clean, df_dirty, df_summary, df_null_cols = segregate_data(df, "PACIENTES")

    # Save
    paths = save_triad_excel(df_clean, df_dirty, df_summary, df_null_cols, "PACIENTES")

    print("--- Generated Files ---")
    for path in paths:
        print(path)

    print(f"\nSummary: {len(df_clean)} clean, {len(df_dirty)} dirty.")

finally:
    reader.close()