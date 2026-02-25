from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from confs.config import dbConfig
from db.db_datasets import (
    create_dataset,
    delete_datasets,
    read_datasets,
    update_visibility,
)
from db.db_datasets_users import (
    create_datasets_user,
    delete_dataset_user,
    read_datasets_user,
)
from db.db_datasets_versions import (
    create_datasets_versions,
    delete_datasets_versions,
    get_max_ds_ver_id,
    read_datasets_versions,
    update_profiling_details,
    list_datasets
)
from db.db_ds_meta import create_ds_meta, delete_ds_meta, read_ds_meta
from db.db_jobs_info import create_job, read_jobs, update_job_status, monitor
from db.db_modules import get_module_name, read_modules
from db.db_pdvu_mapping import create_pdvu_map, current_update, read_pdvu_map, read_pdvu_map, list_datasets_versions, read_pdvu_id, get_ds_ver
from db.db_permission import get_perm_id, read_permissions
from db.db_pii import create_pii
from db.db_proj_ds_mapping import create_proj_ds_map, del_proj_ds_map, read_proj_ds_map, list_datasets_details
from db.db_proj_user import (
    add_user_project,
    change_user_role_project,
    delete_project_user,
    get_project_users,
    get_projects,
)
from db.db_projects import create_proj, delete_proj, edit_project_desc
from db.db_roles import delete_role, read_roles
from db.db_roles_permission_map import (
    create_role,
    create_role_persmission_map,
    delete_role_permission_map,
    read_role_permission_map,
)
from db.db_session_history import get_login_time, get_session_token, set_session_token
from db.db_user_persona import (
    create_user_persona,
    delete_user_persona,
    read_user_persona,
    read_users,
)
from db.db_users import create_user, delete_user, update_pwd, update_user, val_user
from db.db_auto_ml import create_auto_ml
from db.db_model_hub import create_model_hub, delete_model
from db.db_model_version import create_model_version, read_models, delete_model_versions, get_job_id, udpate_pipeline_dict, read_model_version
from db.db_deployment import create_deployment, delete_deployment, read_deployments
from utils.custom_typing import db_session
from utils.logs import get_logger

LOGGER = get_logger()


def get_session(section: str = "postgresql-aws") -> db_session:
    # get session object
    try:

        params = dbConfig(section=section)

        con_str = (
            "postgresql://"
            + str(params["user"])
            + ":"
            + str(params["password"])
            + "@"
            + str(params["host"])
            + "/"
            + str(params["database"])
        )

        engine = create_engine(con_str, echo_pool=False, echo=False, pool_recycle=30)

        conn = engine.connect()
        conn.info
        Session = sessionmaker(bind=engine)
        session = Session()

        return session
    except Exception as error:
        LOGGER.exception(error)
        raise (error)


def close_session(sess: db_session = None) -> None:
    try:
        if sess is not None:
            sess.close()
    except Exception as e:
        LOGGER.exception(e)
