provider "aws" {
  access_key    = "test"
  secret_key    = "test"
  region        = "us-east-1"
  endpoints {
    s3 = "http://localstack:4566"
  }
  s3_use_path_style = true # Force path-style addressing since localstack wont create the DNS entries
}

