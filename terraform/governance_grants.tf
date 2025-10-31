# ============================================================
# Unity Catalog: catalog- and schema-level grants
# ============================================================

# ------------------------------------------------------------
# Catalog-level privileges
# ------------------------------------------------------------
resource "databricks_grants" "catalog_flood_dev" {
  catalog = databricks_catalog.flood_dev.name

  # Give the development user full catalog control
  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
}

# ------------------------------------------------------------
# Bronze schema
# ------------------------------------------------------------
resource "databricks_grants" "schema_bronze" {
  schema = databricks_schema.bronze.id

  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}

# ------------------------------------------------------------
# Silver schema
# ------------------------------------------------------------
resource "databricks_grants" "schema_silver" {
  schema = databricks_schema.silver.id

  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
}

# ------------------------------------------------------------
# Gold schema (environment-aware)
# ------------------------------------------------------------
resource "databricks_grants" "schema_gold" {
  schema = databricks_schema.gold.id

  # In dev: full rights for experimentation
  # In other environments: read-only
  grant {
    principal  = data.databricks_user.cluster_user.user_name
    privileges = var.environment == "dev" ? ["USE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"] : ["USE_SCHEMA", "SELECT"]
  }
}
