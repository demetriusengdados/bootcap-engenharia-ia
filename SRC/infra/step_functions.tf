resource "aws_sfn_state_machine" "pipeline" {

  name = "bootcamp-pipeline"

  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({

    StartAt = "GlueJob",

    States = {

      GlueJob = {

        Type = "Task",

        Resource = "arn:aws:states:::glue:startJobRun",

        Parameters = {

          JobName = aws_glue_job.bronze_to_silver.name

        },

        End = true
      }

    }

  })

}