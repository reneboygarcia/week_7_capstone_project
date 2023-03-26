# Week 7 | Capstone Project
# import
from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from etl_web_to_gcs_bandcamp import etl_parent_web_gcs

# Fetch storage from GitHub
github_block = GitHub.load("bandcamp-github-block")

# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow
gcs_git_dep = Deployment.build_from_flow(
    flow=etl_parent_web_gcs,
    name="bandcamp-flow",
    storage=github_block,
)

print("Successfully deployed Bandcamp Github Block. Check app.prefect.cloud")

# Run main
if __name__ == "__main__":
    gcs_git_dep.apply()

# to deploy
# prefect deployment run el_parent_web_gcs/bandcamp-flow

# format ONLY for params so I cannot forget :)
#  --params '{"years":[2019, 2020], "months": [4, 5, 6, 7, 8, 9, 10, 11, 12, 2, 3, 1]}'
