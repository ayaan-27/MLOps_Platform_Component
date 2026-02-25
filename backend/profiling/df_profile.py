import sys
import pandas as pd, pandas_profiling
from pandas_profiling import ProfileReport
import utils.logs as logs
from typing import Any, Dict, List, NewType

LOGGER = logs.get_logger()

def data_profiling(dataset: pd.DataFrame)-> Dict[str,Any]:
    result = {"status": 0,"msg": "Some Error occured"}
    try:
        if dataset is None:
            result["msg"] = "Dataset not found"
        else:
            profile = ProfileReport(dataset,explorative = True,
                    title = "Data Profile Report",
                    progress_bar = False,
                    interactions = None,
                    missing_diagrams = {
                        "heatmap" : True,
                        "matrix" : False,
                        "bar" : True,
                        "dendrogram" : True,
                        },
                    correlations = {
                        "pearson" :{"calculate":True},
                        "spearman":{"calculate": True},
                        "kendall" :{"calculate": True},
                        "phi_k"   :{"calculate": True},
                        "cramers" :{"calculate": True},
                    },
                )
            report = profile.to_file(output_file='dataset_profile.html')
            result = {"status": 1, "msg": "Data Profile Report succesfully created"}
            LOGGER.info("Dataset Profile report successfully created")
    except Exception as e:
        raise e
    finally:
        return result