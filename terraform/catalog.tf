# ------------------------------------------------------------
# Unity Catalog setup for UK Flood Monitoring project
# ------------------------------------------------------------
resource "databricks_catalog" "flood_dev" {
  name         = "flood_dev"
  comment      = "Catalog for the UK Flood Monitoring project (development)"
  storage_root = databricks_external_location.flood_uc.url

  depends_on = [
    databricks_storage_credential.flood_uc,
    databricks_external_location.flood_uc
  ]
}

# ------------------------------------------------------------
# Schemas within the catalog
# ------------------------------------------------------------

# Bronze: raw ingestion layer
resource "databricks_schema" "bronze" {
  name         = "bronze"
  catalog_name = databricks_catalog.flood_dev.name
  comment      = "Bronze schema for raw UK flood alert data"
}

# Silver: cleaned and curated layer
resource "databricks_schema" "silver" {
  name         = "silver"
  catalog_name = databricks_catalog.flood_dev.name
  comment      = "Silver schema for cleaned and curated flood data"
}

# Gold: analytics-ready layer
resource "databricks_schema" "gold" {
  name         = "gold"
  catalog_name = databricks_catalog.flood_dev.name
  comment      = "Gold schema for analytics-ready flood data"
}
