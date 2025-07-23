from __future__ import annotations
import os
import re
import json
import tempfile
import logging
from typing import Dict, List, Any, Union

import pandas as pd

from bento_etl.transformers.base import BaseTransformer
from bento_etl.transformers.plugins.transformer_phenopacket import build_phenopackets
from bento_etl.config import Config

def is_identifier_field(field_name: str) -> bool:
    return bool(re.search(r'(^id$)|(_id$)', field_name.lower()))

def is_boolean_like(column: pd.Series) -> bool:
    ux = {str(v).strip().lower() for v in column.dropna().unique()}
    ux -= {"unknown", "n/a", "na", ""}
    patterns = [{"true","false"},{"yes","no"},{"1","0"},{"t","f"}]
    return 1 <= len(ux) <= 2 and any(ux.issubset(p) for p in patterns)

def detect_data_type(
    column: pd.Series,
    max_string_length: int,
    min_frequency: int
) -> str:
    dt = column.dtype
    if pd.api.types.is_integer_dtype(dt):
        return "Discrete"
    if pd.api.types.is_float_dtype(dt):
        return "Continuous"
    if is_boolean_like(column):
        return "Boolean"
    if pd.api.types.is_object_dtype(dt) or pd.api.types.is_categorical_dtype(dt):
        freq = column.value_counts(dropna=False)
        mc = freq.max() if not freq.empty else 0
        nonnull = column.dropna().astype(str)
        max_len = nonnull.map(len).max() if not nonnull.empty else 0
        if mc >= min_frequency and max_len <= max_string_length:
            return "Categorical"
    return "Text"

def field_overview(
    df: pd.DataFrame,
    table_name: str,
    max_string_length: int,
    min_frequency: int
) -> pd.DataFrame:
    rows: List[Dict[str,Any]] = []
    for col in df.columns:
        series = df[col]
        n_rows = len(series)
        n_empty = series.isnull().sum()
        n_unique = series.nunique(dropna=True)
        nonnull = series.dropna().astype(str)
        max_len = nonnull.map(len).max() if not nonnull.empty else 0

        dtype_name = "VARCHAR" if series.dtype == object else series.dtype.name
        category = detect_data_type(series, max_string_length, min_frequency)

        if (
            not is_identifier_field(col) and
            category == "Categorical" and
            n_unique < 20
        ):
            seen: List[Any] = []
            for v in series.dropna().unique():
                if v not in seen:
                    seen.append(v)
            values_present = ", ".join(map(str, seen))
        else:
            values_present = ""

        frac_empty  = f"{(n_empty/n_rows):.1%}" if n_rows>0 else "N/A"
        frac_unique = f"{(n_unique/n_rows):.1%}" if n_rows>0 else "N/A"

        rows.append({
            "Table":            table_name,
            "Field":            col,
            "Type":             dtype_name,
            "Category":         category,
            "Values Present":   values_present,
            "Max Length":       max_len,
            "N Rows":           n_rows,
            "Fraction Empty":   frac_empty,
            "N Unique Values":  n_unique,
            "Fraction Unique":  frac_unique,
        })

    return pd.DataFrame.from_records(rows)

def load_json_records(path: str) -> Dict[str, pd.DataFrame]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    grouped: Dict[str, List[Dict[str,Any]]] = {}
    for rec in payload.get("records", []):
        name = rec.get("entityName","Unknown")
        grouped.setdefault(name, []).append(rec.get("data", {}))
    return {name: pd.json_normalize(rows) for name, rows in grouped.items()}

def process_single_json(
    json_path: str,
    output_dir: str,
    max_string_length: int,
    min_frequency: int,
    include_frequency: bool,
    max_fraction_unique: float
) -> None:
    base = os.path.splitext(os.path.basename(json_path))[0]
    os.makedirs(output_dir, exist_ok=True)
    dfs = load_json_records(json_path)

    all_ov: List[pd.DataFrame] = []
    for entity, df in dfs.items():
        tbl = f"{base}_{entity}"
        ov  = field_overview(df, tbl, max_string_length, min_frequency)
        ov["Source File"] = json_path
        all_ov.append(ov)

    if not all_ov:
        raise RuntimeError("No tables to write")

    out_df = pd.concat(all_ov, ignore_index=True)
    csv_path = os.path.join(output_dir, f"{base}_overview.csv")
    out_df.to_csv(csv_path, index=False)
    logging.info(f"Wrote overview to %s", csv_path)

class PhenopacketV2JsonTransformer(BaseTransformer):
    def __init__(self, logger: logging.Logger, config: Config):
        super().__init__(logger, plugin="phenopacket-v2-json")
        self.config = config
        self.files_dir = config.files_dir

    def transform(self, raw: Union[dict, List[Any]]) -> List[Dict]:
        self.logger.info("PhenopacketV2JsonTransformer: start")

        # Handle both dict with "records" and direct List[Dict] input
        if isinstance(raw, dict) and "records" in raw:
            recs = raw["records"]
        elif isinstance(raw, list):
            recs = raw
            self.logger.info("Received list input, treating as records")
        else:
            self.logger.error("Transformer expected dict with 'records' or List[Dict]")
            raise ValueError("Invalid payload for PhenopacketV2JsonTransformer")

        tmp = tempfile.gettempdir()
        json_path = os.path.join(tmp, "api_response_pcgl.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump({"records": recs}, f)
        self.logger.info(f"Wrote temp JSON → {json_path}")

        report_dir = os.path.join(tmp, "phenopacket_report")
        os.makedirs(report_dir, exist_ok=True)
        process_single_json(
            json_path=json_path,
            output_dir=report_dir,
            max_string_length=50,
            min_frequency=2,
            include_frequency=False,
            max_fraction_unique=0.98
        )
        overview_csv = os.path.join(report_dir, "api_response_pcgl_overview.csv")
        if not os.path.exists(overview_csv):
            raise FileNotFoundError(f"Expected {overview_csv}")
        self.logger.info(f"Overview CSV ready at {overview_csv}")

        phenos_out = build_phenopackets(
            overview_csv_path=overview_csv,
            api_json_path=json_path,
            files_dir=self.files_dir
        )
        self.logger.info(f"Built phenopackets JSON → {phenos_out}")

        try:
            with open(phenos_out, "r", encoding="utf-8") as f:
                phenopackets_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to read phenopackets file {phenos_out}: {e}")
            raise

        self.logger.info(f"Transformed {len(phenopackets_data)} phenopackets")
        return phenopackets_data