# CSV Mapping Files

Mapping CSVs define how fields from the PCGL data dictionary map into the ETL JSON models (Katsu xperiments schema and Phenopackets v2).

Current files:
- `mappings/pcgl/pcgl_data_dictionary_prod_V1.0_experiments_mappings.csv`
- `mappings/pcgl/pcgl_data_dictionary_prod_V1.0_phenopacketsV2_mappings.csv`

## Structure
Each row maps: **(Domain, Entity, Field in PCGL) → JSON path to schema**

Conventions:
- Dot notation (`a.b.c`)
- Arrays use `[]`
- Leave target blank if unmapped

## Experiments
Target column: `Experiments`  
Schema: `Katsu-experiments`
Example:  
`experiments.experiment_results[].extra_properties.analysis_type`

## Phenopackets
Target columns: `Phenopackets`
Schema: `Phenopackets V2`
Example:  
`medical_actions[].treatment.cumulative_dose.value`

## Rules
1. Do not change column headers  
2. Match PCGL Domain / Entity / Field exactly  
3. For Phenopackets add the link to the oficial documentation
4. One row per source field  
5. Use valid schema paths only  
