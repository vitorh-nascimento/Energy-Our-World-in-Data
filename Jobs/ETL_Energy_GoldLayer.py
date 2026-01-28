# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


ETL_Energy_GoldLayer = Job.from_dict(
    {
        "name": "ETL_Energy_GoldLayer",
        "tasks": [
            {
                "task_key": "Merge_DIM_Country",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/vitor.nascimento@programmers.com.br/Energy-Our-World-in-Data/Notebooks/3_Gold/3_1_Gold_Dimension_Country",
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
w.jobs.reset(new_settings=ETL_Energy_GoldLayer, job_id=392427361447054)
# or create a new job using: w.jobs.create(**ETL_Energy_GoldLayer.as_shallow_dict())
