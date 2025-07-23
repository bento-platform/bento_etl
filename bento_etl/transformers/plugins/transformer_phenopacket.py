from __future__ import annotations
import json
import os
import logging
import copy
from typing import Dict, Any, List
from collections import defaultdict
import pandas as pd
from jsonschema import Draft7Validator

# Configuration for phenopacket generation
ARRAY_KEY_SUBSTITUTIONS = {
    'biosample': 'biosamples',
    'disease': 'diseases',
    'measurement': 'measurements',
    'procedure': 'procedures',
    'phenotypic_feature': 'phenotypic_features'
}

NOMINAL_ONTOLOGY_MAPPINGS = {
    'vital_status': {
        'Deceased': {'id': 'SNOMED:419099009', 'label': 'Dead'},
        'Alive': {'id': 'SNOMED:263654008', 'label': 'Alive'}
    },
    'gender': {
        'F': {'id': 'SNOMED:2200', 'label': 'Female'},
        'M': {'id': 'SNOMED:2201', 'label': 'Male'}
    },
    'disease': {
        'Poisoning': {'id': 'SNOMED:XXXXX', 'label': 'Poisoning'},
        'Hysterical paralysis': {'id': 'ICD10:F88.89', 'label': 'Hysterical paralysis'}
    },
    'sample_storage': {
        'Frozen in vapour phase': {'id': 'SNOMED:YYYYY', 'label': 'Frozen in vapour phase'}
    },
    'sample_processing': {
        'Cryopreservation in liquid nitrogen (dead tissue)': {
            'id': 'SNOMED:ZZZZZ',
            'label': 'Cryopreservation in liquid nitrogen (dead tissue)'
        }
    }
}

META_DATA = {
    'phenopacket_schema_version': '2.0',
    'created_by': 'C3G Team',
    'submitted_by': 'C3G Team',
    'resources': [
        {
            'name': 'NCBI Taxonomy OBO Edition',
            'version': '2023-09-19',
            'namespace_prefix': 'NCBITaxon',
            'id': 'NCBITaxon:2023-09-19',
            'iri_prefix': 'http://purl.obolibrary.org/obo/NCBITaxon_',
            'url': 'http://purl.obolibrary.org/obo/ncbitaxon/2023-09-19/ncbitaxon.owl'
        },
        {
            'name': 'PCGL Controlled Vocabulary',
            'version': '2020-12-11',
            'namespace_prefix': 'PCGL',
            'id': 'PCGL:2020-12-11',
            'iri_prefix': 'http://example.org/PCGL/',
            'url': 'http://example.org/PCGL/'
        },
        {
            'name': 'SNOMED Clinical Terms',
            'version': '2019-04-11',
            'namespace_prefix': 'SNOMED',
            'id': 'SNOMED:2019-04-11',
            'iri_prefix': 'http://purl.bioontology.org/ontology/SNOMEDCT/',
            'url': 'http://purl.bioontology.org/ontology/SNOMEDCT'
        }
    ]
}

def build_structure_from_schema(node: Dict[str, Any]) -> Any:
    t = node.get('type')
    if t == 'object':
        return {k: build_structure_from_schema(v) for k, v in node.get('properties', {}).items()}
    if t == 'array':
        return []
    if t in ('string', 'number', 'integer'):
        return None
    return None

def get_by_path(obj: Dict, path_parts: List[str]) -> Any:
    cur = obj
    for p in path_parts:
        if isinstance(cur, dict):
            cur = cur.setdefault(p, {})
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return None
    return cur

def get_parent_and_key(obj: Dict, path_parts: List[str]) -> tuple[Any, str]:
    cur = obj
    for p in path_parts[:-1]:
        if isinstance(cur, dict):
            cur = cur.setdefault(p, {})
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return None, None
    return cur, path_parts[-1]

def fix_instance(instance: Dict, errors: List, placeholder_template: Dict, log_handle: Any) -> None:
    from copy import deepcopy
    for error in errors:
        path = list(error.absolute_path)

        if error.validator == 'required':
            missing = set(error.validator_value) - set(error.instance.keys())
            for prop in missing:
                parent, _ = get_parent_and_key(instance, path + [prop])
                if parent is None:
                    continue
                placeholder = deepcopy(get_by_path(placeholder_template, path + [prop]))
                parent[prop] = placeholder
                log_handle.write(
                    f"[FIX] {'.'.join(map(str, path + [prop]))}: "
                    f"added missing property with placeholder {json.dumps(placeholder)}\n"
                )

        elif error.validator == 'type':
            expected = error.validator_value
            parent, key = get_parent_and_key(instance, path)
            if parent is None or key not in parent:
                continue
            original = parent[key]
            field = key

            if expected == 'object':
                if field in NOMINAL_ONTOLOGY_MAPPINGS and isinstance(original, str):
                    if original in NOMINAL_ONTOLOGY_MAPPINGS[field]:
                        m = NOMINAL_ONTOLOGY_MAPPINGS[field][original]
                        placeholder = {'id': m['id'], 'label': m['label']}
                        reason = f"used mapping for '{original}'"
                    else:
                        placeholder = {'id': f"FIXME:{field.upper()}", 'label': original}
                        reason = f"fallback placeholder for '{original}'"
                else:
                    placeholder = deepcopy(get_by_path(placeholder_template, path))
                    reason = "schema-derived object placeholder"
            elif expected == 'array':
                placeholder = []
                reason = "replaced with empty array placeholder"
            else:
                placeholder = deepcopy(get_by_path(placeholder_template, path))
                reason = f"replaced with {expected} placeholder"

            parent[key] = placeholder
            log_handle.write(
                f"[FIX] {'.'.join(map(str, path))}: {reason}; {original!r} -> "
                f"{json.dumps(placeholder)}\n"
            )

        elif error.validator == 'enum':
            parent, key = get_parent_and_key(instance, path)
            if parent is None or key not in parent:
                continue
            allowed = error.validator_value
            original = parent[key]
            if (key in NOMINAL_ONTOLOGY_MAPPINGS and
                isinstance(original, str) and
                original in NOMINAL_ONTOLOGY_MAPPINGS[key]):
                m = NOMINAL_ONTOLOGY_MAPPINGS[key][original]
                placeholder = {'id': m['id'], 'label': m['label']}
                reason = f"enum mapping for '{original}'"
            else:
                placeholder = allowed[0]
                reason = f"set to first allowed enum {allowed[0]!r}"
            parent[key] = placeholder
            log_handle.write(
                f"[FIX] {'.'.join(map(str, path))}: {reason}; replaced {original!r}\n"
            )

        elif error.validator == 'additionalProperties':
            extras = error.params.get('additionalProperties', [])
            parent, _ = get_parent_and_key(instance, path)
            if parent is None:
                continue
            for prop in extras:
                if prop in parent:
                    removed = parent.pop(prop)
                    log_handle.write(
                        f"[FIX] {'.'.join(map(str, path + [prop]))}: "
                        f"removed additional property; value was {json.dumps(removed)}\n"
                    )

def validate_phenopackets(phenopackets: List[Dict], schema_file: str) -> None:
    sch = json.load(open(schema_file, 'r', encoding='utf-8'))[0]['schema']
    v = Draft7Validator(sch)
    placeholder_template = build_structure_from_schema(sch)
    log_file = os.path.join(os.path.dirname(schema_file), 'validation_logs.txt')
    with open(log_file, 'w', encoding='utf-8') as L:
        L.write("Validation Errors and Fixes:\n")
        any_errors = False
        for i, p in enumerate(phenopackets):
            errs = sorted(v.iter_errors(p), key=lambda e: list(e.absolute_path))
            if errs:
                any_errors = True
                L.write(f"\nPhenopacket {i+1} initial errors:\n")
                for e in errs:
                    path = '$.' + '.'.join(map(str, e.absolute_path))
                    L.write(f"  [ERROR] {path}: {e.message}\n")
                fix_instance(p, errs, placeholder_template, L)
                post_errs = sorted(v.iter_errors(p), key=lambda e: list(e.absolute_path))
                if post_errs:
                    L.write(f"\nPhenopacket {i+1} STILL has errors after auto-fix:\n")
                    for e in post_errs:
                        path = '$.' + '.'.join(map(str, e.absolute_path))
                        L.write(f"  [ERROR] {path}: {e.message}\n")
                else:
                    L.write(f"Phenopacket {i+1} passed after auto-fix.\n")
            else:
                L.write(f"\nPhenopacket {i+1} passed schema validation (no fixes needed).\n")
        if any_errors:
            logging.warning(f"Some phenopackets were auto-fixed; see '{log_file}' for details.")
        else:
            logging.info("All phenopackets passed schema validation.")

def read_tables_report(path: str) -> tuple[Dict[str, str], Dict[str, Any], Dict[str, Any]]:
    table_to_file = {}
    nominal_mappings = {}
    ordinal_mappings = {}
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Report not found: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        reader = pd.read_csv(f)
        for _, row in reader.iterrows():
            # Convert all relevant fields to strings to handle floats
            raw_tbl = str(row['Table']) if pd.notna(row['Table']) else ''
            src = str(row['Source File']) if pd.notna(row['Source File']) else ''
            cat = str(row['Category']) if pd.notna(row['Category']) else ''
            fld = str(row['Field']) if pd.notna(row['Field']) else ''
            vals = str(row['Values Present']) if pd.notna(row['Values Present']) else ''
            logging.debug(f"Processing CSV row: Table={raw_tbl}, Field={fld}, Values={vals}, Type={type(row['Values Present'])}")
            
            if not raw_tbl or not src:
                continue
            raw_tbl = raw_tbl.strip()
            src = src.strip()
            prefix = os.path.splitext(os.path.basename(src))[0].lower()
            tbl = raw_tbl.lower()
            if tbl.startswith(prefix + '_'):
                tbl = tbl[len(prefix) + 1:]
            table_to_file[tbl] = src
            cat = cat.strip()
            fld = fld.strip()
            if cat == 'Nominal' and vals:
                nominal_mappings.setdefault(tbl, {})[fld] = {
                    'values': [v.strip() for v in vals.split(',') if v.strip()],
                    'ontology_mappings': {}
                }
            if cat == 'Ordinal' and vals:
                ordinal_mappings.setdefault(tbl, {})[fld] = {
                    'values': [v.strip() for v in vals.split(',') if v.strip()],
                    'order': list(range(len(vals.split(','))))
                }
    return table_to_file, nominal_mappings, ordinal_mappings

def read_json_by_id(path: str, key_field: str = 'submitter_participant_id', table_name: str = None) -> Dict[str, List[Dict]]:
    data = defaultdict(list)
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    for rec in payload.get('records', []):
        if table_name and rec.get('entityName', '').strip().lower() != table_name.lower():
            continue
        row = rec.get('data', {})
        pid = str(row.get(key_field, '')).strip()
        if not pid:
            continue
        cleaned = {k: str(v) for k, v in row.items()}
        data[pid].append(cleaned)
    return data

def add_ontology_mappings(nominal_mappings: Dict[str, Any]) -> Dict[str, Any]:
    for tbl, fields in nominal_mappings.items():
        for fld, data in fields.items():
            for val in data['values']:
                if fld in NOMINAL_ONTOLOGY_MAPPINGS and val in NOMINAL_ONTOLOGY_MAPPINGS[fld]:
                    data['ontology_mappings'][val] = NOMINAL_ONTOLOGY_MAPPINGS[fld][val]
    return nominal_mappings

def read_schema(path: str = 'phenov2-schema-katsu_onliclinical.json') -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)[0]['schema']

def read_mappings(path: str = 'all_mappings_aggregate.json') -> Dict[str, str]:
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)
    m = {}
    for e in raw:
        pc = e.get('PCGL Field', '').strip()
        ph = e.get('Phenopacket Field', '').strip()
        if pc and ph:
            m[pc] = ph
    return m

def apply_dot_path(obj: Dict, dot: str, val: Any) -> None:
    parts = dot.split('.')
    cur = obj
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    final = parts[-1]
    if final in cur and cur[final] not in (None, ""):
        if isinstance(cur[final], list):
            if val not in cur[final]:
                cur[final].append(val)
        else:
            if cur[final] != val:
                cur[final] = [cur[final], val]
    else:
        cur[final] = val

def set_dot_path(obj: Dict, dot: str, val: Any) -> None:
    parts = dot.split('.')
    cur = obj
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = val

def clean_structure(o: Any) -> Any:
    if isinstance(o, dict):
        return {k: clean_structure(v) for k, v in o.items() if not is_empty(clean_structure(v))}
    if isinstance(o, list):
        return [clean_structure(i) for i in o if not is_empty(clean_structure(i))]
    return o

def is_empty(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, str) and not x.strip():
        return True
    if isinstance(x, (list, dict)) and not x:
        return True
    return False

def normalize_pid(pid: str) -> str:
    return pid.replace('_', '').upper().strip()

def build_phenopackets(overview_csv_path: str, api_json_path: str, files_dir: str) -> str:
    # Read config and mappings
    table_to_file, nominal_map, ordinal_map = read_tables_report(overview_csv_path)
    schema = read_schema(os.path.join(files_dir, "phenov2-schema-katsu_onliclinical.json"))
    template = build_structure_from_schema(schema)
    mappings = read_mappings(os.path.join(files_dir, "all_mappings_aggregate.json"))
    nominal_map = add_ontology_mappings(nominal_map)

    # Load JSON data
    data_by_table = {}
    for tbl, src in table_to_file.items():
        data_by_table[tbl] = read_json_by_id(src, key_field='submitter_participant_id', table_name=tbl)

    # Partition mappings
    part_data = data_by_table.get('participant', {})
    participant_fields = set()
    for rows in part_data.values():
        if rows:
            participant_fields = set(rows[0].keys())
            break
    participant_map = {k: mappings[k] for k in mappings if k in participant_fields}
    array_map = {k: mappings[k] for k in mappings if k not in participant_fields}

    # Build phenopackets
    master = {}
    for pid, rows in part_data.items():
        norm = normalize_pid(pid)
        if norm not in master:
            ph = copy.deepcopy(template)
            ph['id'] = f"PHENO_{norm}"
            set_dot_path(ph, 'subject.id', pid)
            set_dot_path(ph, 'meta_data', META_DATA)
            master[norm] = ph
        for row in rows:
            for col, raw in row.items():
                val = str(raw) if raw is not None else ''  # Convert to string, handle None
                if not isinstance(raw, str):
                    logging.debug(f"Converted non-string value for {col}: {raw} to {val}")
                val = val.strip()
                if not val or col not in participant_map:
                    continue
                dot = participant_map[col]
                if dot.startswith('individual.'):
                    dot = 'subject' + dot[len('individual'):]
                fld = dot.split('.')[-1]
                if fld == 'vital_status':
                    enum = val.upper().replace(' ', '_')
                    set_dot_path(master[norm], dot, {'status': enum})
                elif fld in NOMINAL_ONTOLOGY_MAPPINGS and val in NOMINAL_ONTOLOGY_MAPPINGS[fld]:
                    m = NOMINAL_ONTOLOGY_MAPPINGS[fld][val]
                    set_dot_path(master[norm], dot, {'id': m['id'], 'label': m['label']})
                elif ':' in val and ' ' not in val:
                    set_dot_path(master[norm], dot, {'id': val, 'label': val})
                elif fld == 'onset':
                    try:
                        y = int(val)
                        set_dot_path(master[norm], dot, {'age': {'iso8601duration': f'P{y}Y'}})
                    except ValueError:
                        apply_dot_path(master[norm], dot, val)
                else:
                    apply_dot_path(master[norm], dot, val)

    # Array-level data
    for tbl, rows_map in data_by_table.items():
        if tbl == 'participant':
            continue
        for pid, rows in rows_map.items():
            norm = normalize_pid(pid)
            if norm not in master:
                ph = copy.deepcopy(template)
                ph['id'] = f"PHENO_{norm}"
                set_dot_path(ph, 'subject.id', pid)
                set_dot_path(ph, 'meta_data', META_DATA)
                master[norm] = ph
            for row in rows:
                entry = {}
                arr = None
                for col, raw in row.items():
                    val = str(raw) if raw is not None else ''  # Convert to string, handle None
                    if not isinstance(raw, str):
                        logging.debug(f"Converted non-string value for {col}: {raw} to {val}")
                    val = val.strip()
                    if not val or col not in array_map:
                        continue
                    dot = array_map[col]
                    first, *rest = dot.split('.', 1)
                    arr = ARRAY_KEY_SUBSTITUTIONS.get(first.lower())
                    sub = rest[0] if rest else ''
                    if arr:
                        if sub:
                            fld = sub.split('.')[-1]
                            key = first.lower()
                            if key in NOMINAL_ONTOLOGY_MAPPINGS and val in NOMINAL_ONTOLOGY_MAPPINGS[key]:
                                m = NOMINAL_ONTOLOGY_MAPPINGS[key][val]
                                set_dot_path(entry, sub, {'id': m['id'], 'label': m['label']})
                                continue
                            if ':' in val and ' ' not in val:
                                set_dot_path(entry, sub, {'id': val, 'label': val})
                                continue
                            if fld == 'onset':
                                try:
                                    y = int(val)
                                    set_dot_path(entry, sub, {'age': {'iso8601duration': f'P{y}Y'}})
                                    continue
                                except ValueError:
                                    pass
                            if fld == 'term':
                                set_dot_path(entry, sub, {'label': val})
                                continue
                            apply_dot_path(entry, sub, val)
                        else:
                            entry = val
                    else:
                        apply_dot_path(entry, dot, val)
                if arr and not is_empty(entry):
                    master[norm].setdefault(arr, []).append(entry)

    phenos = [clean_structure(p) for p in master.values()]

    # Validate and write phenopackets
    phenos_out = os.path.join(files_dir, "phenopackets_result.json")
    validate_phenopackets(phenos, os.path.join(files_dir, "phenov2-schema-katsu_onliclinical.json"))
    with open(phenos_out, 'w', encoding='utf-8') as wf:
        json.dump(phenos, wf, indent=2)
    logging.info(f"Wrote {len(phenos)} phenopackets to {phenos_out}")

    return phenos_out