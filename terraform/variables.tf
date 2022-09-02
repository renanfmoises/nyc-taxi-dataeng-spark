# Terraform variables.tf file

locals {
    data_lake_bucket = "data_lake"
}

variable "project" {
    description = "The project ID to deploy to"
    default = "nyc-taxi-359621"
}

variable "region" {
    description = "Region for GCP resources."
    default = "southamerica-east1"
    type = string
}

variable "storage_class" {
    description = "Storage class for bucket."
    default = "STANDARD"
}

# variable "BQ_DATASET" {
#     description = "BigQuery dataset where raw data from GCS will be written."
#     default = "nyc_taxi_trips_data_all"
#     type = string
# }
