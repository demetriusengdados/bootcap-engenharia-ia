bootcamp-aws-data-platform/
│
├── infra/
│   ├── provider.tf
│   ├── variables.tf
│   ├── outputs.tf
│   │
│   ├── s3.tf
│   ├── iam.tf
│   ├── glue.tf
│   ├── athena.tf
│   ├── step_functions.tf
│   └── emr.tf
│
├── glue_jobs/
│   └── bronze_to_silver.py
│
├── athena/
│   ├── create_gold.sql
│   └── validation.sql
│
├── step_functions/
│   └── pipeline.json
│
├── emr/
│   └── gold_aggregation.py
│
└── README.md