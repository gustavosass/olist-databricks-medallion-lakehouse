import yaml
from pathlib import Path

def validate_parameters(dataset, layer):

    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError("Invalid stage. Expected one of: bronze, silver, gold")

    if not dataset or not layer:
        raise ValueError("Job parameters required: dataset, catalog, layer")

def load_yaml(dataset, layer):

    #Root path project
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

    with open(f"{PROJECT_ROOT}/conf/{layer}/{dataset}.yaml", "r") as f:
        return yaml.full_load(f)
    
def validate_yaml(config, layer):

    #Validate file_cfg
    file_cfg = config.get("file")
    if not file_cfg:
        raise ValueError("Missing 'file' block in YAML")

    file_format = file_cfg.get("format")
    if file_format is None or str(file_format).strip() == "":
        raise ValueError("Required YAML parameter is empty - Block: file Parameter: 'file_format'")

    #Validate schema_cfg
    schema_cfg = config.get('schema')
    if layer != "bronze" and not schema_cfg:
        raise ValueError("Missing 'schema' block in YAML (required for non-bronze stages)")

def build_options(file_cfg):
    file_format = file_cfg.get("format")
    options = {}

    if file_format == "csv":
        header = file_cfg.get("header", True)
        sep = file_cfg.get("sep")

        if header is not None:
            options["header"] = str(header).lower()

        if sep:
            options["sep"] = sep

    return options
    