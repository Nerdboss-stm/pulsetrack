data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {}

# ─── EMR service role ───────────────────────────────────────────────────────

data "aws_iam_policy_document" "emr_service_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_service" {
  name               = "${var.name_prefix}-emr-service"
  assume_role_policy = data.aws_iam_policy_document.emr_service_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_service_role" {
  role = aws_iam_role.emr_service.name
  # Legacy EMR service role policy — broader EC2 permissions, no tag-condition gating.
  # AmazonEMRServicePolicy_v2 requires every subnet/SG/instance-profile to be tagged
  # with `for-use-with-amazon-emr-managed-policies=true` and is finicky in practice.
  # The legacy policy is still fully supported and is fine for a single-tenant dev cluster.
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# Tag-pass-through permission so EMR can launch EC2 instances on our behalf.
data "aws_iam_policy_document" "emr_service_extra" {
  statement {
    sid    = "PassRoleToEC2InstanceProfile"
    effect = "Allow"

    actions = ["iam:PassRole"]
    resources = [
      aws_iam_role.emr_ec2.arn
    ]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "emr_service_extra" {
  name   = "${var.name_prefix}-emr-service-extra"
  role   = aws_iam_role.emr_service.id
  policy = data.aws_iam_policy_document.emr_service_extra.json
}

# ─── EMR EC2 instance profile (attached to cluster nodes) ───────────────────

data "aws_iam_policy_document" "emr_ec2_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_ec2" {
  name               = "${var.name_prefix}-emr-ec2"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_managed" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# Lakehouse S3 access (read/write for the bucket only)
data "aws_iam_policy_document" "lakehouse_access" {
  statement {
    sid    = "ListLakehouseBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [var.lakehouse_bucket]
  }

  statement {
    sid    = "RWLakehouseObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = ["${var.lakehouse_bucket}/*"]
  }
}

resource "aws_iam_policy" "lakehouse_access" {
  name        = "${var.name_prefix}-lakehouse-access"
  description = "R/W on the PulseTrack lakehouse bucket"
  policy      = data.aws_iam_policy_document.lakehouse_access.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_lakehouse" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.lakehouse_access.arn
}

# Glue catalog access for Iceberg + Hive
data "aws_iam_policy_document" "glue_catalog_access" {
  statement {
    sid    = "GlueRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
      "glue:BatchUpdatePartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_catalog_access" {
  name   = "${var.name_prefix}-glue-catalog-access"
  policy = data.aws_iam_policy_document.glue_catalog_access.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_glue" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

# MSK Serverless IAM auth
data "aws_iam_policy_document" "msk_iam_auth" {
  statement {
    sid    = "MSKConnect"
    effect = "Allow"
    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:AlterCluster",
      "kafka-cluster:DescribeCluster"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "MSKTopicOps"
    effect = "Allow"
    actions = [
      "kafka-cluster:*Topic*",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "MSKGroupOps"
    effect = "Allow"
    actions = [
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "msk_iam_auth" {
  name   = "${var.name_prefix}-msk-iam-auth"
  policy = data.aws_iam_policy_document.msk_iam_auth.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_msk" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.msk_iam_auth.arn
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.name_prefix}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}

# ─── Glue crawler role ──────────────────────────────────────────────────────

data "aws_iam_policy_document" "glue_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_lakehouse" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.lakehouse_access.arn
}
