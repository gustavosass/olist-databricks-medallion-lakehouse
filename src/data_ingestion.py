import yaml

def load_yaml(table):

    with open(f"/Workspace/Repos/gustavosass@gmail.com/olist-databricks-medallion-lakehouse//conf/tables/{table}.yaml", "r") as f:
        return yaml.full_load(f)
    
def validate_yaml(config, stage):

    if stage not in {"bronze", "silver", "gold"}:
        raise ValueError(f"Invalid stage. Expected one of: bronze, silver, gold")

    #Validate file_cfg
    file_cfg = config.get("file")
    if not file_cfg:
        raise ValueError("Missing 'file' block in YAML")

    file_format = file_cfg.get("format")
    if file_format is None or str(file_format).strip() == "":
        raise ValueError("Required YAML parameter is empty - Block: file Parameter: 'file_format'")

    #Validate schema_cfg
    schema_cfg = config.get('schema')
    if stage != "bronze" and not schema_cfg:
        raise ValueError("Missing 'schema' block in YAML (required for non-bronze stages)")

def build_options(file_cfg):

    header = file_cfg.get('header', True)
    sep = file_cfg.get('sep')
    options = {}

    if header is not None:
        options["header"] = str(header).lower()

    if sep:
        options["sep"] = sep
    
    return options
    