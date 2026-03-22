from typing import Tuple
import os, IPython

def get_notebook_parameters():
    
    try:
        dbutils = IPython.get_ipython().user_ns.get("dbutils", None)
        dbutils.widgets.text("dataset", "")
        dataset = dbutils.widgets.get("dataset").strip()

        if not dataset:
            raise ValueError("Parameter 'dataset' is required.")

        return dataset
    except Exception:
        raise