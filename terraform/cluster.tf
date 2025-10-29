# Pick a sensible small node if you didn't set node_type_id
data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_cluster" "single_user_dev" {
  cluster_name            = var.cluster_name
  spark_version           = var.spark_version
  node_type_id            = coalesce(var.node_type_id, data.databricks_node_type.smallest.id)
  num_workers             = var.num_workers
  autotermination_minutes = var.autotermination_minutes

  data_security_mode = "SINGLE_USER"
  single_user_name   = var.cluster_user_email
}

output "single_user_cluster_id" {
  value       = databricks_cluster.single_user_dev.id
  description = "ID of the single-user development cluster"
}

