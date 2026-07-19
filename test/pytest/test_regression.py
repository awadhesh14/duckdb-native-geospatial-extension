import duckdb
import pytest
import re
import os

# Assuming the extension is built in build/release/extension/geoparquet_scanner/geoparquet_scanner.duckdb_extension
EXTENSION_PATH = "build/release/extension/geoparquet_scanner/geoparquet_scanner.duckdb_extension"

@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    
    # Check if the extension exists before trying to load it
    if os.path.exists(EXTENSION_PATH):
        c.execute(f"LOAD '{EXTENSION_PATH}'")
    else:
        # Fallback to just loading by name if installed in the duckdb directory
        try:
            c.execute("LOAD geoparquet_scanner")
        except duckdb.Error:
            pytest.skip("Could not load geoparquet_scanner extension")
    
    return c

def test_file_level_pruning(con):
    # Query completely outside the global bounds
    # Global bounds for test_buildings are approx [-180, -87.68]
    # xmin > 180 should prune the entire file.
    explain_output = con.execute("EXPLAIN ANALYZE SELECT count(*) FROM read_geoparquet('data/test_buildings.parquet') WHERE bbox.xmin > 180").fetchall()
    
    # explain_output is a list of tuples, usually [(plan, )] or similar depending on duckdb version.
    # In newer duckdb, EXPLAIN ANALYZE returns multiple columns, the second column is often the plan string.
    plan_str = ""
    for row in explain_output:
        for col in row:
            if isinstance(col, str) and "TABLE_SCAN" in col:
                plan_str = col
                break
        if plan_str:
            break
            
    # If the output format is a single large string
    if not plan_str and explain_output and explain_output[0]:
        plan_str = str(explain_output[0])

    assert "TABLE_SCAN" in plan_str, "Could not find TABLE_SCAN in explain output"
    
    # We expect something like: 0 rows
    # Let's extract the rows processed by the TABLE_SCAN node.
    # We look for a line containing TABLE_SCAN and the next few lines for " X rows"
    
    # This is a basic regex to find the rows emitted by TABLE_SCAN.
    # Since duckdb's EXPLAIN format changes, we look for "0 rows" directly after TABLE_SCAN.
    # The file level pruning ensures 0 rows are read from the parquet file.
    
    table_scan_part = plan_str.split("TABLE_SCAN")[1]
    
    # Look for the first occurrence of `\d+ rows`
    match = re.search(r'(\d+)\s+rows?', table_scan_part)
    assert match is not None, "Could not find row count in TABLE_SCAN output"
    
    rows_scanned = int(match.group(1))
    assert rows_scanned == 0, f"Expected 0 rows to be scanned due to file-level pruning, but got {rows_scanned}"

def test_row_group_pruning(con):
    # Query that intersects the file but prunes some row groups.
    explain_output = con.execute("EXPLAIN ANALYZE SELECT count(*) FROM read_geoparquet('data/test_buildings.parquet') WHERE bbox.xmin > -100").fetchall()
    
    plan_str = ""
    for row in explain_output:
        for col in row:
            if isinstance(col, str) and "TABLE_SCAN" in col:
                plan_str = col
                break
        if plan_str:
            break
            
    if not plan_str and explain_output and explain_output[0]:
        plan_str = str(explain_output[0])

    table_scan_part = plan_str.split("TABLE_SCAN")[1]
    match = re.search(r'(\d+(?:,\d+)*)\s+rows?', table_scan_part)
    assert match is not None, "Could not find row count in TABLE_SCAN output"
    
    # Parse number with commas if any
    rows_scanned = int(match.group(1).replace(",", ""))
    
    # The total file has 5,000,000 rows.
    # Row groups have max 122,880 rows.
    # If it scanned exactly 5,000,000, pruning failed.
    # The expected number is around 4.7 million for this specific filter.
    assert rows_scanned > 0, "Expected more than 0 rows to be scanned"
    assert rows_scanned < 5000000, f"Expected less than 5,000,000 rows to be scanned due to row-group pruning, but got {rows_scanned}"
