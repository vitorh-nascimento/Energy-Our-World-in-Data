# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


ETL_Energy_BronzeLayer = Job.from_dict(
    {
        "name": "ETL_Energy_BronzeLayer",
        "tasks": [
            {
                "task_key": "Ingest_Energy_to_Bronze",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/vitor.nascimento@programmers.com.br/Energy-Our-World-in-Data/Notebooks/1_Bronze/1_1_Bronze_Insert_Energy",
                    "source": "WORKSPACE",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=ETL_Energy_BronzeLayer, job_id=1017191368972612)
# or create a new job using: w.jobs.create(**ETL_Energy_BronzeLayer.as_shallow_dict())
