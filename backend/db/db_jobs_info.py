import time
import json

from sqlalchemy import Boolean, Column, ForeignKey, Integer
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import query
from sqlalchemy.sql.elements import and_
from sqlalchemy.sql.operators import is_precedent

from db.db_metadata import Auto_repr, Base
from db.db_pdvu_mapping import Pdvu_mapping
from db.db_proj_ds_mapping import Proj_ds_mapping
from db.db_proj_user import Project_user
from utils.custom_typing import Any, Dict, db_session
from utils.logs import get_logger

LOGGER = get_logger()

# Job Status
# 0 Not Started
# 1 Running
# 2 complete
# 3 error


class Jobs_info(Base, Auto_repr):
    __tablename__ = "jobs_info"
    job_id = Column(Integer, primary_key=True)
    pdvu_id = Column(Integer, ForeignKey("pdvu_mapping.pk"), nullable=False)
    creation_time = Column(postgresql.BIGINT, nullable=False, default=int(time.time()))
    start_time = Column(postgresql.BIGINT, nullable=False, default=-1)
    end_time = Column(postgresql.BIGINT, nullable=False, default=-1)
    job_status = Column(Integer, default=0)
    job_options = Column(postgresql.JSON, nullable=False)
    parent_job_id = Column(Integer, default=-1)
    job_type = Column(postgresql.VARCHAR(100), nullable=False)
    is_deleted = Column(Boolean, default=False)


def create_job(
    sess: db_session,
    pdvu_mapping: int,
    job_options: json,
    job_type: json,
    parent_job_id: int = None,
    commit: bool = True,
) -> Dict[str, Any]:
    # Creates user
    try:
        job_ = Jobs_info()
        job_.pdvu_id = pdvu_mapping
        job_.job_options = job_options
        job_.job_type = job_type
        job_.creation_time = int(time.time())
        if parent_job_id is not None:
            job_.parent_job_id = parent_job_id

        sess.add(job_)

        if commit:
            sess.commit()
        else:
            sess.flush()
        return {"status": 1, "msg": job_.job_id}
    except Exception as e:
        LOGGER.exception(e)
        sess.rollback()
        raise e


def monitor(sess: db_session, job_id: int):
    try:
        query = sess.query(Jobs_info).filter(Jobs_info.job_id == job_id)
        jobs = []
        if query.count() != 0:
            for q in query:
                jobs.append(q.to_dict())
            return {"status": 1, "msg": jobs}
        else:
            return {"status": 0, "msg": "No records found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def read_jobs(
    sess: db_session,
    job_id: int = None,
    dataset_id: int = None,
    version_id: int = None,
    project_id: int = None,
    user_id: int = None,
    pdvu_ids: list = None,
    job_status: int = None,
    parent_job_id: int = None,
    mlflow_runid: int = None,
    deleted: bool = False,
) -> Dict[str, Any]:
    LOGGER.info("returns all the elements of table jobs")
    try:
        query = (
            sess.query(
                Jobs_info.job_id,
                Jobs_info.pdvu_id,
                Jobs_info.creation_time,
                Jobs_info.start_time,
                Jobs_info.end_time,
                Jobs_info.job_status,
                Jobs_info.job_options,
                Jobs_info.job_type,
                Jobs_info.parent_job_id,
                Jobs_info.is_deleted,
                Pdvu_mapping.dataset_id,
                Pdvu_mapping.version_id,
                Project_user.project_id,
                Project_user.user_id,
            )
            .join(Pdvu_mapping, Jobs_info.pdvu_id == Pdvu_mapping.pk)
            .join(Project_user, Pdvu_mapping.project_user_id == Project_user.pk)
        )

        if not deleted:
            query = query.filter(Jobs_info.is_deleted == False)

        if job_id is not None:
            query = query.filter(Jobs_info.job_id == job_id)

        if dataset_id is not None and version_id is None:
            query = query.filter(Pdvu_mapping.dataset_id == dataset_id)

        if dataset_id is not None and version_id is not None:
            query = query.filter(
                and_(
                    Pdvu_mapping.dataset_id == dataset_id,
                    Pdvu_mapping.version_id == version_id,
                )
            )

        if user_id is not None:
            query = query.filter(Project_user.user_id == user_id)

        if project_id is not None:
            query = query.filter(Project_user.project_id == project_id)

        if pdvu_ids is not None:
            query = query.filter(Jobs_info.pdvu_id.in_(pdvu_ids))

        if job_status is not None:
            query = query.filter(Jobs_info.job_status == job_status)

        if parent_job_id is not None:
            query = query.filter(Jobs_info.parent_job_id == parent_job_id)

        if mlflow_runid is not None:
            query = query.filter(Jobs_info.mlflow_runid == mlflow_runid)

        if query.count() != 0:
            jobs_list = []
            for i in query:
                jobs = {}
                jobs["job_id"] = i[0]
                jobs["pdvu_id"] = i[1]
                jobs["creation_time"] = i[2]
                jobs["start_time"] = i[3]
                jobs["end_time"] = i[4]
                jobs["job_status"] = i[5]
                jobs["job_options"] = i[6]
                jobs["job_type"] = i[7]
                jobs["parent_job_id"] = i[8]
                jobs["is_deleted"] = i[9]
                jobs["dataset_id"] = i[10]
                jobs["version_id"] = i[11]
                jobs["project_id"] = i[12]
                jobs["user_id"] = i[13]
                jobs_list.append(jobs)
            return {"status": 1, "msg": jobs_list}
        else:
            return {"status": 0, "msg": "Job(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)


def update_job_status(
    sess: db_session, job_id: int, job_status: int, job_options: json = None
) -> Dict[str, Any]:
    # Updates the job status in db
    try:
        query = sess.query(Jobs_info).filter(Jobs_info.job_id == job_id)

        if query.count() != 0:
            temp = query.first()
            temp.job_status = job_status
            if job_options is not None:
                temp.job_options = job_options
            if job_status > 1:
                temp.end_time = int(time.time())
            elif job_status == 1:
                temp.start_time = int(time.time())
            sess.commit()
            return {"status": 1, "msg": "Job Updated"}
        else:
            return {"status": 0, "msg": "Job(s) not found"}
    except Exception as e:
        LOGGER.exception(e)
        raise (e)
