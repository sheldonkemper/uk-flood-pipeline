# ============================================================
# Bronze Volumes for raw files and checkpoints
# ============================================================

resource "databricks_volume" "bronze_raw" {
  name         = "raw_payloads"
  catalog_name = databricks_catalog.flood_dev.name
  schema_name  = databricks_schema.bronze.name
  volume_type  = "MANAGED"
  comment      = "Raw API payloads from UK Flood Monitoring API"
}

resource "databricks_volume" "bronze_checkpoints" {
  name         = "checkpoints"
  catalog_name = databricks_catalog.flood_dev.name
  schema_name  = databricks_schema.bronze.name
  volume_type  = "MANAGED"
  comment      = "DLT checkpoint storage for streaming pipelines"
}

# ============================================================
# Volume-level grants
# ============================================================

# Grant you read/write access to raw payloads
resource "databricks_grants" "volume_bronze_raw" {
  volume = databricks_volume.bronze_raw.id

  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}

# Grant you write access to checkpoints
resource "databricks_grants" "volume_bronze_checkpoints" {
  volume = databricks_volume.bronze_checkpoints.id

  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = ["READ_VOLUME", "WRITE_VOLUME"]
  }
}
