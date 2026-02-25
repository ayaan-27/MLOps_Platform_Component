from celery import Celery

import db.db as db
from datasets import datasets
from preprocessing.preprocessor_master import pre_processor_controller
from utils.logs import get_logger

LOGGER = get_logger()


app = Celery(
    "celery_tasks",
    broker="amqp://pace_user:nextlabs123@localhost/pace",
    backend="db+postgresql://celery:nextlabs123@localhost/celery",
)


@app.task
def pre_processing(job_id: int):
    sess = None
    try:
        sess = db.get_session()

        # set job started
        res = db.update_job_status(sess, job_id=job_id, job_status=1)

        if res["status"] == 1:

            # get job details
            res = db.read_jobs(sess, job_id=job_id)
            if res["status"] != 1:
                Exception(res["msg"])

            else:
                dataset_id = res["msg"][0]["dataset_id"]
                version_id = res["msg"][0]["version_id"]
                user_id = res["msg"][0]["user_id"]
                project_id = res["msg"][0]["project_id"]
                data_loc = datasets.io.download_file(
                    dataset_id=dataset_id, version_id=version_id
                )
                data = datasets.io.read_file(dataset_loc=data_loc)

                job_confs = res["msg"][0]["job_options"]

                data = pre_processor_controller(
                    data, job_confs, project_id=str(project_id)
                )

                # create new version of the data
                res = datasets.create_version(
                    dataset_id=dataset_id,
                    user_id=user_id,
                    prev_version_id=version_id,
                    job_id=job_id,
                    dataset=data,
                )
                new_version_id = res["msg"]

                if res["status"] != 1:
                    Exception(res["msg"])

                else:
                    # set job to done
                    res = db.update_job_status(sess, job_id=job_id, job_status=2)

                    if res["status"] != 1:
                        Exception(res["msg"])
                    else:
                        res = datasets.create_pdvu_map(
                            dataset_id=dataset_id,
                            version_id=new_version_id,
                            user_id=user_id,
                            project_id=project_id,
                        )

                        if res["status"] != 1:
                            Exception(res["msg"])

    except Exception as e:
        LOGGER.exception(e)
        if sess is not None:
            res = db.update_job_status(sess, job_id=job_id, job_status=3)
        else:
            pre_processing.apply_async((job_id), queue="pre_process")
    finally:
        db.close_session(sess)
