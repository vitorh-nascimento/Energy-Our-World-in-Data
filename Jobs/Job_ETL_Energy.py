# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


ETL_Energy = Job.from_dict(
    {
        "name": "ETL_Energy",
        "tasks": [
            {
                "task_key": "Ingest_Bronze",
                "run_job_task": {
                    "job_id": 1017191368972612,
                },
            },
            {
                "task_key": "Transform_Silver",
                "depends_on": [
                    {
                        "task_key": "Ingest_Bronze",
                    },
                ],
                "run_job_task": {
                    "job_id": 759408319110101,
                },
            },
            {
                "task_key": "Load_Gold",
                "depends_on": [
                    {
                        "task_key": "Transform_Silver",
                    },
                ],
                "run_job_task": {
                    "job_id": 392427361447054,
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=ETL_Energy, job_id=597019887562078)
# or create a new job using: w.jobs.create(**ETL_Energy.as_shallow_dict())
