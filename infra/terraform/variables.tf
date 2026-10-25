variable "project_name" {
  type    = string
  default = "paysim-graph-fraud"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "neo4j_instance_type" {
  type        = string
  default     = "r6i.large"
  description = "Memory-optimized EC2 for Neo4j Community in Docker."
}

variable "neo4j_password" {
  type      = string
  sensitive = true
}

variable "redshift_admin_password" {
  type      = string
  sensitive = true
}

variable "redshift_node_type" {
  type    = string
  default = "ra3.xlplus"
}

variable "redshift_nodes" {
  type    = number
  default = 1
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 2
}
