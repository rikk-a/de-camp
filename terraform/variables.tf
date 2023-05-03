locals {
  data_lake_bucket = "de_data_lake"
}

variable "project" {
  description = "de-camp-v1"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "US"
  type = string
}


variable "credentials" {
    description = "GCP credentials filepath"
    default = "/home/vi/projects/de_camp/credentials/de-camp-v1-f45c6758b882.json"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}


variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "space_mission_launches"
}


variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "mission_launches"
}