output "s3_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "neo4j_private_ip" {
  value = aws_instance.neo4j.private_ip
}

output "neo4j_bolt" {
  value = "bolt://${aws_instance.neo4j.private_ip}:7687"
}

output "neo4j_instance_id" {
  value = aws_instance.neo4j.id
}

output "kinesis_stream" {
  value = aws_kinesis_stream.transactions.name
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.main.endpoint
}

output "glue_batch_job" {
  value = aws_glue_job.batch.name
}

output "glue_stream_job" {
  value = aws_glue_job.stream.name
}

output "glue_cypher_job" {
  value = aws_glue_job.cypher.name
}

output "step_function" {
  value = aws_sfn_state_machine.batch.name
}

output "step_function_arn" {
  value = aws_sfn_state_machine.batch.arn
}

output "power_bi_view" {
  value = "fraud.v_latest_flags"
}
