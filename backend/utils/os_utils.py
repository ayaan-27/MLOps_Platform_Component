import os


def get_env_variables(key):
    # TODO: Exception handling, logging
    key_val = None
    if "TRACKING_URI" in key:
        key_val = "http://3.17.196.36:5000"
    else:
        key_val = os.environ.get(key)
    return key_val
