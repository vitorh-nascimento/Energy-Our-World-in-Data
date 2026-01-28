# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


ETL_Energy_SilverLayer = Job.from_dict(
    {
        "name": "ETL_Energy_SilverLayer",
        "tasks": [
            {
                "task_key": "Transform_Country",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/vitor.nascimento@programmers.com.br/Energy-Our-World-in-Data/Notebooks/2_Silver/2_2_Silver_Tranform_Country",
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
w.jobs.reset(new_settings=ETL_Energy_SilverLayer, job_id=759408319110101)
# or create a new job using: w.jobs.create(**ETL_Energy_SilverLayer.as_shallow_dict())
