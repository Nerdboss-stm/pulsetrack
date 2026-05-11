terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # ~> 5.95 — pinned to the latest 5.x stable line. Bumped from
      # 5.70 alongside the EMR 7.2.0 -> 7.13.0 release-label upgrade
      # for support of newer EMR features (e.g., the latest
      # ``aws_emr_cluster`` IAM-pass-through behavior, MSK Serverless
      # ACL improvements). Stay on ~> 5.x until we have a deliberate
      # plan to validate against 6.x's breaking changes.
      version = "~> 5.95"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project       = "PulseTrack"
      Environment   = var.environment
      ManagedBy     = "Terraform"
      AutoTerminate = "true"
    }
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name_prefix = "pulsetrack-${var.environment}"
  suffix      = random_id.suffix.hex
}

module "networking" {
  source = "./modules/networking"

  name_prefix         = local.name_prefix
  environment         = var.environment
  aws_region          = var.aws_region
  vpc_cidr            = var.vpc_cidr
  public_subnet_cidrs = var.public_subnet_cidrs
}

module "storage" {
  source = "./modules/storage"

  name_prefix = local.name_prefix
  suffix      = local.suffix
  environment = var.environment
}

module "iam" {
  source = "./modules/iam"

  name_prefix      = local.name_prefix
  environment      = var.environment
  lakehouse_bucket = module.storage.bucket_arn
}

module "catalog" {
  source = "./modules/catalog"

  name_prefix      = local.name_prefix
  environment      = var.environment
  lakehouse_bucket = module.storage.bucket_name
  glue_role_arn    = module.iam.glue_role_arn
}

module "kafka" {
  source = "./modules/kafka"

  name_prefix             = local.name_prefix
  environment             = var.environment
  subnet_ids              = module.networking.public_subnet_ids
  kafka_security_group_id = module.networking.kafka_security_group_id
}

module "compute" {
  source = "./modules/compute"

  name_prefix               = local.name_prefix
  environment               = var.environment
  release_label             = var.emr_release_label
  master_instance_type      = var.emr_master_instance_type
  core_instance_type        = var.emr_core_instance_type
  core_instance_count       = var.emr_core_instance_count
  core_spot_bid_price       = var.emr_core_spot_bid_price
  ebs_volume_size_gb        = var.emr_ebs_volume_size_gb
  idle_timeout_seconds      = var.emr_idle_timeout_seconds
  subnet_id                 = module.networking.public_subnet_ids[0]
  emr_security_group_id     = module.networking.emr_security_group_id
  service_security_group_id = module.networking.emr_service_security_group_id
  key_pair_name             = var.key_pair_name
  service_role_arn          = module.iam.emr_service_role_arn
  instance_profile_arn      = module.iam.emr_instance_profile_arn
  bootstrap_bucket          = module.storage.bucket_name
  lakehouse_bucket          = module.storage.bucket_name
  bootstrap_object_key      = module.storage.bootstrap_object_key

  depends_on = [module.storage, module.iam]
}

module "monitoring" {
  source = "./modules/monitoring"

  name_prefix      = local.name_prefix
  environment      = var.environment
  aws_region       = var.aws_region
  alert_email      = var.alert_email
  budget_limit_usd = var.budget_limit_usd
  emr_cluster_id   = module.compute.cluster_id
  lakehouse_bucket = module.storage.bucket_name
}

# ─────────────────────────────────────────────────────────────────────────────
# Secrets — AWS Secrets Manager + KMS, replaces the prior `.env`-only pattern.
# Wired after the iam module so the EMR EC2 role exists for policy attachment.
# Bootstrap secret values via:  python scripts/bootstrap_secrets.py
# ─────────────────────────────────────────────────────────────────────────────
module "secrets" {
  source = "./modules/secrets"

  name_prefix       = local.name_prefix
  environment       = var.environment
  emr_ec2_role_name = module.iam.emr_ec2_role_name

  depends_on = [module.iam]
}
