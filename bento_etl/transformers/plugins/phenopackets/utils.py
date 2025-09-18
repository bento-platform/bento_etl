from collections import defaultdict
from jsonschema import validate, ValidationError
from typing import Any

META_DATA = {
    "phenopacket_schema_version": "2.0",
    "created_by": "C3G Team",
    "submitted_by": "C3G Team",
    "resources": [
        {
            "name": "NCBI Taxonomy OBO Edition",
            "version": "2023-09-19",
            "namespace_prefix": "NCBITaxon",
            "id": "NCBITaxon:2023-09-19",
            "iri_prefix": "http://purl.obolibrary.org/obo/NCBITaxon_",
            "url": "http://purl.obolibrary.org/obo/ncbitaxon/2023-09-19/ncbitaxon.owl",
        },
        {
            "name": "SNOMED Clinical Terms",
            "version": "2019-04-11",
            "namespace_prefix": "SNOMED",
            "id": "SNOMED:2019-04-11",
            "iri_prefix": "http://purl.bioontology.org/ontology/SNOMEDCT/",
            "url": "http://purl.bioontology.org/ontology/SNOMEDCT",
        },
        {
            "name": "NCI Thesaurus OBO Edition",
            "version": "25.08d",
            "namespace_prefix": "NCIT",
            "id": "NCIT:25.08d",
            "iri_prefix": "http://purl.obolibrary.org/obo/NCIT_",
            "url": "http://purl.obolibrary.org/obo/ncit.owl",
        },
    ],
}


def get_leaf_mappings(mappings, leaves=None):
    if leaves is None:
        leaves = []
    for m in mappings:
        if "sub_mappings" in m:
            get_leaf_mappings(m["sub_mappings"], leaves)
        else:
            leaves.append(m)
    return leaves


def set_nested(obj, path, value):
    parts = path.split(".")
    current = obj
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def get_value(row, m, ontology_mappings):
    field = m["api_field"].split(".")[1]
    raw_value = row.get(field)
    if raw_value is None:
        return None
    t = m.get("type")
    if t == "ontology_class":
        key = m["ontology_key"]
        ont = ontology_mappings.get(key, {}).get(str(raw_value), None)
        if ont and ont.get("id"):
            return {"id": ont["id"], "label": ont.get("label", "")}
        else:
            return None
    elif "value_map" in m:
        return m["value_map"].get(raw_value, raw_value)
    elif "transform" in m:
        func = globals()[m["transform"]]
        context = m.get("context", {})
        return func(raw_value, row, context)
    else:
        return raw_value


def merge_items(array, dedup_key):
    if not dedup_key:
        return array
    merged = {}
    key_parts = dedup_key.split(".")
    for item in array:
        key_current = item
        skip = False
        for part in key_parts:
            if key_current is None or not isinstance(key_current, dict):
                skip = True
                break
            key_current = key_current.get(part)
        if skip or key_current is None:
            continue
        key = str(key_current)
        if key not in merged:
            merged[key] = {}
        for k, v in item.items():
            if isinstance(v, dict) and k in merged[key]:
                merged[key][k].update(v)
            else:
                merged[key][k] = v
    return list(merged.values())


def prune_empty(obj):
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            prune_empty(obj[k])
            if isinstance(obj[k], (dict, list)) and not obj[k]:
                del obj[k]
    elif isinstance(obj, list):
        for item in obj:
            prune_empty(item)


def age_to_iso(raw_value, row, context):
    age = int(raw_value)
    return f"P{age}Y"


def curie_to_ontology(raw_value, row, context):
    id_ = row.get(context.get("id_field"), raw_value)
    label = row.get(context.get("label_field"), "")
    if id_ and not label:
        label = f"Label not provided for {id_}"
    elif label and not id_:
        id_ = "ID not provided"
    return {"id": str(id_), "label": str(label)} if id_ or label else None


def combine_processing_storage(raw_value, row, context):
    processing = row.get("sample_processing", "")
    storage = row.get("sample_storage", "") or raw_value
    return f"{processing} - {storage}".strip(" -")


def group_by_participant(input_data, group_field):
    participant_entities = defaultdict(lambda: defaultdict(list))
    participant_ids = set()
    for table, rows in input_data.items():
        for row in rows:
            p_id = row.get(group_field)
            if p_id:
                participant_entities[p_id][table].append(row)
                participant_ids.add(p_id)
    return participant_entities, participant_ids


def associate_referenced_rows(
    participant_entities, participant_ids, input_data, group_field
):
    for p_id in participant_ids:
        for table in input_data:
            for row in input_data[table]:
                if row.get(group_field) is None and any(
                    p_id in str(v) for v in row.values()
                ):
                    participant_entities[p_id][table].append(row)


def process_non_array_mappings(
    phenopacket, participant_entities, non_array_mappings, ontology_mappings
):
    for m in non_array_mappings:
        table = m["api_field"].split(".")[0]
        rows = participant_entities.get(table, [])
        value = None
        for row in rows:
            temp_value = get_value(row, m, ontology_mappings)
            if temp_value is not None:
                value = temp_value
                break
        if value is not None:
            set_nested(phenopacket, m["schema_field"], value)


def process_array_mappings(
    phenopacket,
    participant_entities,
    array_sources,
    ontology_mappings,
    array_item_schemas,
    per_field_arrays,
):
    for array_name, sources in array_sources.items():
        array = phenopacket.get(array_name, [])
        phenopacket[array_name] = array
        array_item_schema = array_item_schemas.get(array_name, {})
        for table, ms in sources.items():
            rows = participant_entities.get(table, [])
            if not rows:
                continue
            if array_name in per_field_arrays:
                grouped_ms = defaultdict(list)
                for m in ms:
                    field = m["api_field"].split(".")[1]
                    grouped_ms[field].append(m)
                for row in rows:
                    for field, group_ms in grouped_ms.items():
                        item = {}
                        for m in group_ms:
                            value = get_value(row, m, ontology_mappings)
                            if value is not None:
                                item_path = m["schema_field"].split("[]")[1][1:]
                                set_nested(item, item_path, value)
                        if item:
                            array.append(item)
            else:
                for row in rows:
                    item = {}
                    for m in ms:
                        value = get_value(row, m, ontology_mappings)
                        if value is not None:
                            item_path = m["schema_field"].split("[]")[1][1:]
                            set_nested(item, item_path, value)
                    if item:
                        array.append(item)


def build_phenopacket(
    p_id,
    participant_entities,
    leaf_mappings,
    non_array_mappings,
    array_sources,
    ontology_mappings,
    array_item_schemas,
    per_field_arrays,
    array_dedup_keys,
):
    phenopacket = {
        "id": p_id.replace(" ", "_"),
        "subject": {"id": p_id.replace(" ", "_")},
    }
    phenopacket["meta_data"] = META_DATA.copy()
    process_non_array_mappings(
        phenopacket, participant_entities, non_array_mappings, ontology_mappings
    )
    process_array_mappings(
        phenopacket,
        participant_entities,
        array_sources,
        ontology_mappings,
        array_item_schemas,
        per_field_arrays,
    )
    for array_name in array_dedup_keys:
        if array_name in phenopacket:
            phenopacket[array_name] = merge_items(
                phenopacket[array_name], array_dedup_keys[array_name]
            )
    prune_empty(phenopacket)
    return phenopacket


def perform_transformation(
    raw: Any, schema: dict, mappings: dict, ontology_mappings: dict, logger
) -> list[dict]:
    array_item_schemas = {}
    for prop, defn in schema["properties"].items():
        if defn.get("type") == "array":
            item_schema = defn.get("items", {})
            if "properties" in item_schema or "oneOf" in item_schema:
                array_item_schemas[prop] = item_schema
    array_dedup_keys = {
        "diseases": "term.id",
        "exposures": "exposure_code.id",
        "measurements": "assay.id",
        "biosamples": "id",
        "phenotypic_features": "type.id",
        "procedures": "code.id",
        "treatments": "agent.id",
        "medical_actions": "treatment.agent.id",
    }
    per_field_arrays = ["phenotypic_features"]
    input_data = defaultdict(list)
    for record in raw:
        if "data" in record:
            entity = record["entityName"]
            data = record["data"]
            input_data[entity].append(data)
    mappings = mappings["phenopacket"]
    group_mapping = next((m for m in mappings if m.get("is_group_key")), None)
    if not group_mapping:
        raise ValueError("No group key found")
    group_field = group_mapping["api_field"].split(".")[1]
    participant_entities, participant_ids = group_by_participant(
        input_data, group_field
    )
    associate_referenced_rows(
        participant_entities, participant_ids, input_data, group_field
    )
    leaf_mappings = get_leaf_mappings(mappings)
    non_array_mappings = [m for m in leaf_mappings if "[]" not in m["schema_field"]]
    array_sources = defaultdict(lambda: defaultdict(list))
    for m in leaf_mappings:
        if "[]" in m["schema_field"]:
            array_name = m["schema_field"].split("[]")[0]
            table = m["api_field"].split(".")[0]
            array_sources[array_name][table].append(m)
    phenopackets = []
    for p_id in sorted(participant_entities):
        phenopacket = build_phenopacket(
            p_id,
            participant_entities[p_id],
            leaf_mappings,
            non_array_mappings,
            array_sources,
            ontology_mappings,
            array_item_schemas,
            per_field_arrays,
            array_dedup_keys,
        )
        try:
            validate(instance=phenopacket, schema=schema)
            logger.info(f"Validated for {p_id}")
        except ValidationError as e:
            logger.error(f"Validation error for {p_id}: {e.message}")
        phenopackets.append(phenopacket)
    print(raw, "RAW_1")
    print(phenopackets, "PhenoPz")
    return phenopackets
