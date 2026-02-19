resource "aws_emr_cluster" "cluster" {

  name = "bootcamp-emr"

  release_label = "emr-6.10.0"

  applications = [

    "Spark"

  ]


  service_role = "EMR_DefaultRole"


  ec2_attributes {

    instance_profile = "EMR_EC2_DefaultRole"

  }


  master_instance_type = "m5.xlarge"

  core_instance_type = "m5.xlarge"

  core_instance_count = 2

}