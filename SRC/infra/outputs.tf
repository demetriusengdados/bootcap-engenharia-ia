output "data_lake_bucket" {

  value = aws_s3_bucket.data_lake.bucket
}


output "glue_job_name" {

  value = aws_glue_job.bronze_to_silver.name
}


output "athena_workgroup" {

  value = aws_athena_workgroup.bootcamp.name
}


output "step_function" {

  value = aws_sfn_state_machine.pipeline.name
}


output "emr_cluster_id" {

  value = aws_emr_cluster.cluster.id
}