terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.27"
    }
  }
  
}

provider "aws" {
	profile = "default"
	region = "us-west-2"

}

resource "aws_sqs_queue" "terraform_queue" {
  name                      = "quote-updates"
  delay_seconds             = 0
  max_message_size          = 32768
  message_retention_seconds = 86400
  receive_wait_time_seconds = 10
  tags = {
    Project = "rrg-creator"
  }
}