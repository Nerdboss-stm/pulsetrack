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
  # The v2 service policy gates EC2/SG operations on this tag, both on the
  # role itself and on the resources it acts upon. Pair with the matching
  # tag on subnets + EMR SGs (set in modules/networking/main.tf).
  tags = {
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
}

# EMR service role policy — KEPT for reference, NOT attached.
#
# Operator history (all attempted on this branch, all rejected by EMR):
#
#   1. ``AmazonEMRServicePolicy_v2`` (AWS-managed, AWS-recommended) — tried
#      on EMR 7.2.0 and 7.13.0 with the required
#      ``for-use-with-amazon-emr-managed-policies=true`` tag set on every
#      subnet, EMR-managed SG, IAM role, instance profile. EMR rejects
#      with VALIDATION_ERROR: "Service role has insufficient EC2 permissions".
#      ``aws iam simulate-principal-policy`` with the same context returns
#      ``Decision: allowed`` for every action EMR could need — so the policy
#      is internally correct, but EMR's preflight validator does something
#      else (not documented).
#
#   2. Custom inline policy (this data block, action list mirrors v2 plus
#      the legacy lifecycle actions v2 dropped — DetachNetworkInterface,
#      DetachVolume, DeleteVolume, ModifyImageAttribute, plus
#      DescribeSecurityGroupRules and GetSecurityGroupsForVpc). Same
#      VALIDATION_ERROR. The action list isn't the issue — EMR's preflight
#      asserts a shape we can't satisfy without an AWS-published spec for
#      what it actually checks.
#
# Resolution: attach ``AmazonElasticMapReduceRole`` (legacy / v1) below.
# This is what ``aws emr create-default-roles`` creates today — AWS itself
# ships it as the default for new clusters, so it remains supported. The
# trade-off is broader EC2 permissions than v2's tag-gated shape, but for a
# single-tenant analytics cluster in our own VPC the blast-radius math
# favors "ship something that works" over "ship something tightly scoped
# that EMR rejects".
#
# This data block stays in source as documentation of the attempt — keeping
# it makes it easy for a future operator (or AWS support engagement) to
# resume from the right starting point. It isn't referenced by any
# ``aws_iam_policy`` or ``aws_iam_role_policy_attachment``.
data "aws_iam_policy_document" "emr_service_custom_unused" {
  # ── Read-only describes — planning, validation, health checks ──────────
  # No conditions; describes are non-mutating and EMR's pre-flight relies on
  # broad visibility (it queries subnets it might use, AMIs in the region,
  # capacity reservations, etc.). Mirrors v2's ListActionsForEC2Resources
  # statement plus the few read APIs v2 dropped that legacy retained.
  statement {
    sid    = "EC2DescribeAndPlanningReads"
    effect = "Allow"
    actions = [
      "ec2:DescribeAccountAttributes",
      "ec2:DescribeAvailabilityZones",
      "ec2:DescribeCapacityReservations",
      "ec2:DescribeDhcpOptions",
      "ec2:DescribeImages",
      "ec2:DescribeInstances",
      "ec2:DescribeInstanceStatus",
      "ec2:DescribeInstanceTypeOfferings",
      "ec2:DescribeKeyPairs",
      "ec2:DescribeLaunchTemplates",
      "ec2:DescribeNetworkAcls",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribePlacementGroups",
      "ec2:DescribePrefixLists",
      "ec2:DescribeRouteTables",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSpotInstanceRequests",
      "ec2:DescribeSpotPriceHistory",
      "ec2:DescribeSubnets",
      "ec2:DescribeTags",
      "ec2:DescribeVolumes",
      "ec2:DescribeVolumeStatus",
      "ec2:DescribeVpcAttribute",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeVpcEndpointServices",
      "ec2:DescribeVpcs",
    ]
    resources = ["*"]
  }

  # ── RunInstances + CreateFleet — gated on subnet/SG carrying our tag ──
  # Mirrors v2's CreateInTaggedNetwork. The networking module sets this tag
  # on all subnets and EMR-managed SGs.
  statement {
    sid    = "RunInstancesIntoTaggedNetwork"
    effect = "Allow"
    actions = [
      "ec2:RunInstances",
      "ec2:CreateFleet",
      "ec2:CreateLaunchTemplate",
      "ec2:CreateLaunchTemplateVersion",
      "ec2:CreateNetworkInterface",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:*:*:subnet/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:security-group/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/for-use-with-amazon-emr-managed-policies"
      values   = ["true"]
    }
  }

  # ── RunInstances + CreateFleet — non-tagged dependents ────────────────
  # Image (AMI), key-pair, placement-group, dedicated-host, capacity-reservation,
  # network-interface, fleet, resource-groups: these don't carry our tag because
  # they're EMR/AWS-managed. Mirrors v2's ResourcesToLaunchEC2.
  statement {
    sid    = "RunInstancesUntaggedDependents"
    effect = "Allow"
    actions = [
      "ec2:RunInstances",
      "ec2:CreateFleet",
      "ec2:CreateLaunchTemplate",
      "ec2:CreateLaunchTemplateVersion",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:*:*:network-interface/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:image/ami-*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:key-pair/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:capacity-reservation/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:placement-group/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:fleet/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:dedicated-host/*",
      "arn:${data.aws_partition.current.partition}:resource-groups:*:*:group/*",
    ]
  }

  # ── RunInstances + CreateFleet — instance/volume/launch-template carry
  # our tag at creation time (EMR sets it via TagSpecifications). This is
  # how v2 enforces the "EMR's resources stay tagged" invariant.
  statement {
    sid    = "RunInstancesCreateTaggedInstancesAndVolumes"
    effect = "Allow"
    actions = [
      "ec2:RunInstances",
      "ec2:CreateFleet",
      "ec2:CreateLaunchTemplate",
      "ec2:CreateLaunchTemplateVersion",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:*:*:instance/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:volume/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:launch-template/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/for-use-with-amazon-emr-managed-policies"
      values   = ["true"]
    }
  }

  # ── CreateTags — only at the moment of create, only on EMR resources ──
  # Mirrors v2's TagOnCreateTaggedEMRResources.
  statement {
    sid    = "CreateTagsAtCreate"
    effect = "Allow"
    actions = ["ec2:CreateTags"]
    resources = [
      "arn:${data.aws_partition.current.partition}:ec2:*:*:instance/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:volume/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:network-interface/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:launch-template/*",
      "arn:${data.aws_partition.current.partition}:ec2:*:*:security-group/*",
    ]
    condition {
      test     = "StringEquals"
      variable = "ec2:CreateAction"
      values = [
        "RunInstances",
        "CreateFleet",
        "CreateLaunchTemplate",
        "CreateNetworkInterface",
        "CreateSecurityGroup",
      ]
    }
  }

  # ── Manage EMR-tagged resources lifecycle ─────────────────────────────
  # Terminate, modify, delete — only on resources carrying our tag.
  # Mirrors v2's ManageEMRTaggedResources + ManageTagsOnEMRTaggedResources,
  # plus the lifecycle actions legacy (AmazonElasticMapReduceRole) included
  # but v2 dropped: DetachNetworkInterface, DetachVolume, DeleteVolume,
  # ModifyImageAttribute. EMR's pre-flight validator (which doesn't
  # populate tag context) appears to check some of these unconditionally
  # — keeping them avoids an opaque "insufficient EC2 permissions" reject.
  statement {
    sid    = "ManageEMRTaggedResources"
    effect = "Allow"
    actions = [
      "ec2:TerminateInstances",
      "ec2:ModifyInstanceAttribute",
      "ec2:DeleteLaunchTemplate",
      "ec2:DeleteNetworkInterface",
      "ec2:DetachNetworkInterface",
      "ec2:DetachVolume",
      "ec2:DeleteVolume",
      "ec2:ModifyImageAttribute",
      "ec2:CreateTags",
      "ec2:DeleteTags",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/for-use-with-amazon-emr-managed-policies"
      values   = ["true"]
    }
  }

  # ── EC2 — actions EMR's pre-flight validates without populated tag
  # context. The simulator returns implicitDeny for the tag-conditioned
  # equivalents above when context isn't supplied, and EMR's preflight
  # appears to interpret implicitDeny as missing-permission rather than
  # context-pending. We grant the cleanup-side actions here without the
  # tag condition; the actual cluster-launched resources are still
  # discoverable only via tags by the upstream code, so the reduction in
  # blast radius from tag-scoping the most-mutating CREATE actions is
  # preserved. Mirrors what AWS does in v2 with their hardcoded role
  # names — they implicitly trust EMR not to terminate non-EMR instances.
  statement {
    sid    = "EC2PreFlightCleanup"
    effect = "Allow"
    actions = [
      "ec2:CreateSecurityGroup",
      "ec2:DeleteSecurityGroup",
      "ec2:CreatePlacementGroup",
      "ec2:DeletePlacementGroup",
      "ec2:DescribeSecurityGroupRules",
      "ec2:GetSecurityGroupsForVpc",
    ]
    resources = ["*"]
  }

  # ── Security group rule management — only on tagged SGs ──────────────
  # Mirrors v2's ManageSecurityGroups. Both EMR-managed master/slave SGs
  # carry the tag (set in modules/networking).
  statement {
    sid    = "ManageTaggedSecurityGroupRules"
    effect = "Allow"
    actions = [
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:RevokeSecurityGroupEgress",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/for-use-with-amazon-emr-managed-policies"
      values   = ["true"]
    }
  }

  # ── Spot Instance APIs ───────────────────────────────────────────────
  # Used because the core fleet is spot-bid (see tfvars
  # ``emr_core_spot_bid_price``). v2 dropped these in favor of CreateFleet,
  # but EMR's older RequestSpotFleet code path still appears for some
  # configurations — keep them for compatibility.
  statement {
    sid    = "SpotInstanceLifecycle"
    effect = "Allow"
    actions = [
      "ec2:CancelSpotInstanceRequests",
      "ec2:RequestSpotInstances",
    ]
    resources = ["*"]
  }

  # ── IAM read for self-introspection ──────────────────────────────────
  # EMR's pre-flight queries the role + instance profile it's been given.
  # Read-only.
  statement {
    sid    = "IAMSelfIntrospect"
    effect = "Allow"
    actions = [
      "iam:GetInstanceProfile",
      "iam:GetRole",
      "iam:GetRolePolicy",
      "iam:ListInstanceProfiles",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:CreateServiceLinkedRole",
    ]
    resources = ["*"]
    # Service-linked-role creation gated to EC2 Spot — v2 omits this but
    # legacy + the EMR docs both require it for spot-fleet-based clusters.
    condition {
      test     = "StringLike"
      variable = "iam:AWSServiceName"
      values = [
        "spot.amazonaws.com",
        "elasticmapreduce.amazonaws.com",
      ]
    }
  }

  # ── CloudWatch metrics + alarms (managed scaling, future use) ────────
  statement {
    sid    = "CloudWatchMetricsAndAlarms"
    effect = "Allow"
    actions = [
      "cloudwatch:PutMetricData",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:DeleteAlarms",
    ]
    resources = ["*"]
  }

  # ── Application Auto Scaling (used if managed scaling is enabled) ───
  statement {
    sid    = "ApplicationAutoScaling"
    effect = "Allow"
    actions = [
      "application-autoscaling:RegisterScalableTarget",
      "application-autoscaling:DeregisterScalableTarget",
      "application-autoscaling:DescribeScalableTargets",
      "application-autoscaling:DescribeScalingActivities",
      "application-autoscaling:DescribeScalingPolicies",
      "application-autoscaling:PutScalingPolicy",
      "application-autoscaling:DeleteScalingPolicy",
    ]
    resources = ["*"]
  }

  # ── S3 access to the lakehouse for bootstrap + log writes ────────────
  # Tightly scoped to the project bucket. Matches what
  # AmazonElasticMapReduceforEC2Role grants the EC2 instance profile, but
  # the service role also needs S3 read for bootstrap action download.
  statement {
    sid    = "S3LakehouseAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:PutObject",
    ]
    resources = [
      var.lakehouse_bucket,
      "${var.lakehouse_bucket}/*",
    ]
  }
}

# Attach AWS-managed AmazonElasticMapReduceRole (legacy / v1).
# See the long comment on ``data.aws_iam_policy_document.emr_service_custom_unused``
# above for the v2 + custom-policy attempts and why we settled here.
resource "aws_iam_role_policy_attachment" "emr_service_role" {
  role       = aws_iam_role.emr_service.name
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
  # Required by AmazonEMRServicePolicy_v2 for the iam:PassRole call from
  # the service role to the EC2 instance profile.
  tags = {
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
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
  # AmazonEMRServicePolicy_v2 condition: the service role can only PassRole
  # to instance profiles tagged here (the EC2 nodes that EMR launches).
  tags = {
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
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

# ─── Glacierbase lock table (DynamoDB) ──────────────────────────────────────
#
# Concurrency lock for the migration framework — see migrations/lock.py.
# WHOOP Glacierbase blog post explicitly calls out a per-catalog lock to
# prevent concurrent migration runs racing on the same table set.
#
# Schema:
#   * Partition key: ``catalog`` (string) — one lock row per catalog.
#   * ``holder`` — who currently holds the lock (user@host:pid).
#   * ``acquired_at`` — unix epoch seconds when acquired.
#   * ``expires_at`` — TTL attribute, DynamoDB auto-reaps stale locks if the
#     holding process crashes without releasing.
#
# Pay-per-request billing: lock acquisitions are O(once per migration run),
# orders of magnitude below the provisioned-capacity threshold. PPR is the
# right billing mode for this access pattern.
resource "aws_dynamodb_table" "glacierbase_lock" {
  name         = "${var.name_prefix}-glacierbase-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "catalog"

  attribute {
    name = "catalog"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  # Point-in-time recovery on the lock table is overkill (lock rows are
  # ephemeral) but cheap and matches the "production-defensible defaults"
  # bar set by the rest of this module.
  point_in_time_recovery {
    enabled = true
  }

  # DynamoDB encrypts at rest by default with AWS-owned keys (no charge,
  # always available). We deliberately do NOT enable a custom KMS key for
  # the lock table — lock rows are ephemeral state, not regulated data,
  # and the per-request KMS charges + key-availability dependency would
  # add risk without benefit. (Earlier `enabled = true` with no
  # `kms_key_arn` resolved to a stale account-level alias and blocked
  # CreateTable — see git history.)

  tags = {
    Name = "${var.name_prefix}-glacierbase-locks"
  }
}

# IAM policy granting the EMR EC2 role read/write/delete on the lock table.
# Migrations run from EMR master via spark-submit, which assumes the
# instance profile — that's what acquires/releases the lock.
data "aws_iam_policy_document" "glacierbase_lock_access" {
  statement {
    sid    = "GlacierbaseLockTable"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:UpdateItem",
      "dynamodb:DescribeTable",
    ]
    resources = [aws_dynamodb_table.glacierbase_lock.arn]
  }
}

resource "aws_iam_policy" "glacierbase_lock_access" {
  name   = "${var.name_prefix}-glacierbase-lock-access"
  policy = data.aws_iam_policy_document.glacierbase_lock_access.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_glacierbase_lock" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.glacierbase_lock_access.arn
}
