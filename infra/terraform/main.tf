data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

resource "random_id" "suffix" {
  byte_length = 3
}

locals {
  name      = "${var.project_name}-${var.environment}"
  bucket    = "${var.project_name}-${var.environment}-${random_id.suffix.hex}"
  azs       = slice(data.aws_availability_zones.available.names, 0, 2)
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "${local.name}-vpc" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 4, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true
  tags                    = { Name = "${local.name}-public-${count.index}" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 4, count.index + 2)
  availability_zone = local.azs[count.index]
  tags              = { Name = "${local.name}-private-${count.index}" }
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_s3_bucket" "data" {
  bucket        = local.bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "prefixes" {
  for_each = toset([
    "raw/paysim/",
    "curated/transactions/",
    "serving/flags/",
    "scripts/cypher/",
    "glue/jars/",
    "checkpoints/stream/",
  ])
  bucket  = aws_s3_bucket.data.id
  key     = each.value
  content = ""
}

resource "aws_s3_object" "cypher_rules" {
  bucket = aws_s3_bucket.data.id
  key    = "scripts/cypher/fraud_rules.cypher"
  source = "${path.module}/../../cypher/fraud_rules.cypher"
  etag   = filemd5("${path.module}/../../cypher/fraud_rules.cypher")
}

resource "aws_s3_object" "sample_csv" {
  bucket = aws_s3_bucket.data.id
  key    = "raw/paysim/paysim_sample.csv"
  source = "${path.module}/../../data/sample/paysim_sample.csv"
  etag   = filemd5("${path.module}/../../data/sample/paysim_sample.csv")
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
}

resource "aws_kinesis_stream" "transactions" {
  name             = "${local.name}-transactions"
  shard_count      = 1
  retention_period = 24
}

resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "${local.name}-to-s3"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose.arn
    bucket_arn          = aws_s3_bucket.data.arn
    prefix              = "raw/stream/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    error_output_prefix = "raw/stream-errors/"
    buffering_interval  = 60
    buffering_size      = 5
    compression_format  = "UNCOMPRESSED"
  }

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.transactions.arn
    role_arn           = aws_iam_role.firehose.arn
  }
}

resource "aws_secretsmanager_secret" "neo4j" {
  name = "${local.name}-neo4j"
}

resource "aws_secretsmanager_secret_version" "neo4j" {
  secret_id = aws_secretsmanager_secret.neo4j.id
  secret_string = jsonencode({
    username = "neo4j"
    password = var.neo4j_password
    url      = "bolt://${aws_instance.neo4j.private_ip}:7687"
  })
}

resource "aws_security_group" "neo4j" {
  name   = "${local.name}-neo4j"
  vpc_id = aws_vpc.main.id

  ingress {
    description     = "Bolt from Glue"
    from_port       = 7687
    to_port         = 7687
    protocol        = "tcp"
    security_groups = [aws_security_group.glue.id]
  }

  ingress {
    description = "Browser from VPC"
    from_port   = 7474
    to_port     = 7474
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  ingress {
    description = "SSH from VPC"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "glue" {
  name   = "${local.name}-glue"
  vpc_id = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "glue_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  security_group_id        = aws_security_group.glue.id
  source_security_group_id = aws_security_group.glue.id
}

resource "aws_security_group" "redshift" {
  name   = "${local.name}-redshift"
  vpc_id = aws_vpc.main.id

  ingress {
    description = "Redshift from VPC (Power BI gateway / Glue)"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "neo4j_ec2" {
  name = "${local.name}-neo4j-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "neo4j_ssm" {
  role       = aws_iam_role.neo4j_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "neo4j" {
  name = "${local.name}-neo4j"
  role = aws_iam_role.neo4j_ec2.name
}

resource "aws_instance" "neo4j" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.neo4j_instance_type
  subnet_id              = aws_subnet.private[0].id
  vpc_security_group_ids = [aws_security_group.neo4j.id]
  iam_instance_profile   = aws_iam_instance_profile.neo4j.name

  user_data = templatefile("${path.module}/user_data_neo4j.sh.tftpl", {
    neo4j_password = var.neo4j_password
  })

  user_data_replace_on_change = true

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  tags = { Name = "${local.name}-neo4j" }
}

resource "aws_iam_role" "glue" {
  name = "${local.name}-glue"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_data" {
  name = "${local.name}-glue-data"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = [aws_kinesis_stream.transactions.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.neo4j.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:CreateNetworkInterface", "ec2:DeleteNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DescribeVpcEndpoints", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role" "firehose" {
  name = "${local.name}-firehose"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "${local.name}-firehose"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:AbortMultipartUpload", "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:PutObject"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = [aws_kinesis_stream.transactions.arn]
      }
    ]
  })
}

resource "aws_iam_role" "redshift" {
  name = "${local.name}-redshift"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "${local.name}-redshift-s3"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
    }]
  })
}

resource "aws_iam_role" "states" {
  name = "${local.name}-states"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "states" {
  name = "${local.name}-states"
  role = aws_iam_role.states.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_glue_connection" "vpc" {
  name = "${local.name}-vpc"
  connection_type = "NETWORK"
  physical_connection_requirements {
    availability_zone      = aws_subnet.private[0].availability_zone
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = aws_subnet.private[0].id
  }
}

resource "aws_glue_job" "batch" {
  name     = "${local.name}-batch-ingest"
  role_arn = aws_iam_role.glue.arn
  depends_on = [
    aws_s3_object.batch_script,
    aws_s3_object.cypher_rules,
    aws_s3_object.sample_csv,
  ]


  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  connections       = [aws_glue_connection.vpc.name]

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data.bucket}/glue/jobs/batch_ingest.py"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--RAW_PATH"                         = "s3://${aws_s3_bucket.data.bucket}/raw/paysim/"
    "--CURATED_PATH"                     = "s3://${aws_s3_bucket.data.bucket}/curated/transactions/"
    "--NEO4J_URL"                        = "bolt://${aws_instance.neo4j.private_ip}:7687"
    "--NEO4J_USER"                       = "neo4j"
    "--NEO4J_PASSWORD"                   = var.neo4j_password
    "--datalake-formats"                 = "iceberg"
    "--additional-python-modules"        = "neo4j"
    "--user-jars-first"                  = "true"
    "--extra-jars"                       = "s3://${aws_s3_bucket.data.bucket}/glue/jars/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar"
  }
}

resource "aws_s3_object" "batch_script" {
  bucket = aws_s3_bucket.data.id
  key    = "glue/jobs/batch_ingest.py"
  source = "${path.module}/../../glue/jobs/batch_ingest.py"
  etag   = filemd5("${path.module}/../../glue/jobs/batch_ingest.py")
}

resource "aws_s3_object" "stream_script" {
  bucket = aws_s3_bucket.data.id
  key    = "glue/jobs/stream_ingest.py"
  source = "${path.module}/../../glue/jobs/stream_ingest.py"
  etag   = filemd5("${path.module}/../../glue/jobs/stream_ingest.py")
}

resource "aws_s3_object" "cypher_script" {
  bucket = aws_s3_bucket.data.id
  key    = "glue/jobs/run_cypher_and_unload.py"
  source = "${path.module}/../../glue/jobs/run_cypher_and_unload.py"
  etag   = filemd5("${path.module}/../../glue/jobs/run_cypher_and_unload.py")
}

resource "aws_glue_job" "stream" {
  name     = "${local.name}-stream-ingest"
  role_arn = aws_iam_role.glue.arn

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  connections       = [aws_glue_connection.vpc.name]
  timeout           = 2880

  command {
    name            = "gluestreaming"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data.bucket}/glue/jobs/stream_ingest.py"
  }

  default_arguments = {
    "--job-language"              = "python"
    "--STREAM_NAME"               = aws_kinesis_stream.transactions.name
    "--CURATED_PATH"              = "s3://${aws_s3_bucket.data.bucket}/curated/transactions_stream/"
    "--CHECKPOINT_PATH"           = "s3://${aws_s3_bucket.data.bucket}/checkpoints/stream/"
    "--NEO4J_URL"                 = "bolt://${aws_instance.neo4j.private_ip}:7687"
    "--NEO4J_USER"                = "neo4j"
    "--NEO4J_PASSWORD"            = var.neo4j_password
    "--AWS_REGION"                = var.aws_region
    "--extra-jars"                = "s3://${aws_s3_bucket.data.bucket}/glue/jars/neo4j-connector-apache-spark_2.12-5.3.1_for_spark_3.jar"
  }
}

resource "aws_glue_job" "cypher" {
  name         = "${local.name}-cypher-unload"
  role_arn     = aws_iam_role.glue.arn
  glue_version = "3.0"
  max_capacity = 1
  connections  = [aws_glue_connection.vpc.name]

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data.bucket}/glue/jobs/run_cypher_and_unload.py"
  }

  default_arguments = {
    "--NEO4J_URL"                 = "bolt://${aws_instance.neo4j.private_ip}:7687"
    "--NEO4J_USER"                = "neo4j"
    "--NEO4J_PASSWORD"            = var.neo4j_password
    "--FLAGS_S3_URI"              = "s3://${aws_s3_bucket.data.bucket}/serving/flags/"
    "--CYPHER_S3_URI"             = "s3://${aws_s3_bucket.data.bucket}/scripts/cypher/fraud_rules.cypher"
    "--additional-python-modules" = "neo4j"
  }
}

resource "aws_redshift_subnet_group" "main" {
  name       = "${local.name}-rs"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier = "${local.name}-rs"
  node_type          = var.redshift_node_type
  number_of_nodes    = var.redshift_nodes
  cluster_type       = var.redshift_nodes == 1 ? "single-node" : "multi-node"

  database_name   = "fraud"
  master_username = "admin"
  master_password = var.redshift_admin_password
  port            = 5439

  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  iam_roles                 = [aws_iam_role.redshift.arn]

  publicly_accessible = false
  encrypted           = true
  skip_final_snapshot = true
}

resource "aws_sfn_state_machine" "batch" {
  name     = "${local.name}-batch"
  role_arn = aws_iam_role.states.arn
  definition = jsonencode({
    Comment = "PaySim batch: Spark ingest to Neo4j, Cypher flags, S3 for Redshift"
    StartAt = "BatchIngest"
    States = {
      BatchIngest = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.batch.name
        }
        Next = "CypherUnload"
      }
      CypherUnload = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.cypher.name
        }
        End = true
      }
    }
  })
}
