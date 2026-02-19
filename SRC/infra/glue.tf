resource "aws_glue_catalog_database" "bronze" {

  name = "bronze"
}

resource "aws_glue_catalog_database" "silver" {

  name = "silver"
}

resource "aws_glue_catalog_database" "gold" {

  name = "gold"
}



resource "aws_glue_job" "bronze_to_silver" {

  name = "bronze-to-silver"

  role_arn = aws_iam_role.glue_role.arn


  command {

    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/scripts/bronze_to_silver.py"

    python_version = "3"
  }

  glue_version = "4.0"

  worker_type = "G.1X"

  number_of_workers = 2
}