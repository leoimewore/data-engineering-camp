terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
# Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
#  credentials = 
  project = "dataengineerproject-448203"
  region  = "us-central1"
}



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "dataengineerproject-448203-gcp-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "dataengineerproject-448203-bigquery-dataset"
  project    = "dataengineerproject-448203"
  location   = "US"
}