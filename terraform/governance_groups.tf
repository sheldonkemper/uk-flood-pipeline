# ------------------------------------------------------------
# Workspace groups for Unity Catalog governance
# ------------------------------------------------------------
resource "databricks_group" "data_engineers" {
  display_name = "data_engineers"
}

resource "databricks_group" "data_scientists" {
  display_name = "data_scientists"
}

# ------------------------------------------------------------
# Look up the user who will run the SINGLE_USER dev cluster
# ------------------------------------------------------------
data "databricks_user" "cluster_user" {
  user_name = var.cluster_user_email
}

# ------------------------------------------------------------
# Ensure the dev user can build pipelines (engineer role)
# ------------------------------------------------------------
resource "databricks_group_member" "engineer_me" {
  group_id  = databricks_group.data_engineers.id
  member_id = data.databricks_user.cluster_user.id
}
