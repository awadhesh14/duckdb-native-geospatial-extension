import pyarrow as pa
import pyarrow.parquet as pq
import json
import os

OUT_DIR = "data/edge_cases"
os.makedirs(OUT_DIR, exist_ok=True)

# 1. missing_geo_metadata.parquet
# A file with some dummy spatial-looking columns, but no "geo" key in the metadata.
table_missing_geo = pa.table({
    "id": [1, 2, 3],
    "geometry": [b"wkb1", b"wkb2", b"wkb3"],
    "bbox": [
        {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
        {"xmin": -10.0, "ymin": -10.0, "xmax": 0.0, "ymax": 0.0},
        {"xmin": 100.0, "ymin": 100.0, "xmax": 110.0, "ymax": 110.0}
    ]
})
pq.write_table(table_missing_geo, os.path.join(OUT_DIR, "missing_geo_metadata.parquet"))

# 2. missing_bbox_column.parquet
# Valid "geo" metadata, but the table itself lacks a "bbox" struct.
table_missing_bbox_col = pa.table({
    "id": [1, 2, 3],
    "geometry": [b"wkb1", b"wkb2", b"wkb3"]
})
geo_meta = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Polygon"],
            "bbox": [-10.0, -10.0, 110.0, 110.0]
        }
    }
}
meta = {b"geo": json.dumps(geo_meta).encode("utf-8")}
table_missing_bbox_col = table_missing_bbox_col.replace_schema_metadata(meta)
pq.write_table(table_missing_bbox_col, os.path.join(OUT_DIR, "missing_bbox_column.parquet"))

# 3. malformed_geo_metadata.parquet
# "geo" key exists, but contains invalid JSON.
table_malformed = pa.table({
    "id": [1, 2, 3],
    "geometry": [b"wkb1", b"wkb2", b"wkb3"],
    "bbox": [
        {"xmin": 0.0, "ymin": 0.0, "xmax": 10.0, "ymax": 10.0},
        {"xmin": -10.0, "ymin": -10.0, "xmax": 0.0, "ymax": 0.0},
        {"xmin": 100.0, "ymin": 100.0, "xmax": 110.0, "ymax": 110.0}
    ]
})
meta_malformed = {b"geo": b"{ invalid json : [ ]"}
table_malformed = table_malformed.replace_schema_metadata(meta_malformed)
pq.write_table(table_malformed, os.path.join(OUT_DIR, "malformed_geo_metadata.parquet"))

# 4. empty_file.parquet
# Valid "geo" metadata but 0 rows (which translates to 0 row groups).
table_empty = pa.table({
    "id": pa.array([], type=pa.int64()),
    "geometry": pa.array([], type=pa.binary()),
    "bbox": pa.array([], type=pa.struct([
        pa.field("xmin", pa.float64()),
        pa.field("ymin", pa.float64()),
        pa.field("xmax", pa.float64()),
        pa.field("ymax", pa.float64())
    ]))
})
table_empty = table_empty.replace_schema_metadata(meta)
pq.write_table(table_empty, os.path.join(OUT_DIR, "empty_file.parquet"))

print("Successfully generated edge case data.")
