environment = "dev"
location    = "uksouth"

databricks_workspace_url = "https://adb-3577787633845022.2.azuredatabricks.net"

# Cluster
cluster_name            = "single-user-dev"
spark_version           = "13.3.x-scala2.12"
num_workers             = 1
autotermination_minutes = 10
cluster_user_email      = "sheldon.kemper@outlook.com"

# Optional override
# node_type_id = "Standard_D4ds_v5"
