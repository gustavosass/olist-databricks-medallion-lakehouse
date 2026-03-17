from typing import Tuple
import os


def get_notebook_parameters(dataset_widget=None):

    try:
        dbutils.widgets.text("dataset")
        dataset = dbutils.widgets.get("dataset").strip()

    except Exception:
        dataset = dataset_widget.value.strip()

    if not dataset:
        raise ValueError("Parameter 'dataset' is required.")
    
    return dataset

def get_root_path() -> str:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return f"/Volumes"
    else:
        return f"data"