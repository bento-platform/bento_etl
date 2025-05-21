from fastapi import APIRouter

"""
Jobs router plan:
/jobs       [GET]       => list submitted jobs
/jobs       [POST]      => submit a job
/jobs/{ID}  [GET]       => get a specific job
/jobs/{ID}  [DELETE]    => kill a job if it is running
"""

# TODO: Job model describing a submitable ETL pipeline job
# TODO: Add DB to keep track of jobs
# TODO: Run pipelines as a background tast, figure out dep injection for ETL components

trigger_router = APIRouter("/jobs")


@trigger_router.post("")
async def trigger_job():
    raise NotImplementedError()
