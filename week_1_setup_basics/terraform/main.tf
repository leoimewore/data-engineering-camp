terraform {
  required_providers {
    google = {
      
      source = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  project     = "dataengineerproject-448203"
  region      = "us-central1"
}


resource "google_storage_bucket" "demo-bucket" {
  name          = "dataengineerproject-448203-gcp-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}




