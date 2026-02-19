resource "aws_athena_workgroup" "bootcamp" {

  name = "bootcamp-workgroup"

  configuration {

    result_configuration {

      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
    }
  }
}