locals {
  data_lake_bucket = "demo_data_lake"
}

variable "project" {
  description = "degroup4ue"
}

variable "region" {
  description = "Location"
  default = "europe-west3"
  type = string
}

variable "storage_class" {
  description = "Storage class"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "great_expectations_bigquery_example"
}
