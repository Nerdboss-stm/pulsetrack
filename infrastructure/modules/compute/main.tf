locals {
  configurations = templatefile("${path.module}/configurations.json", {
    lakehouse_bucket = var.lakehouse_bucket
  })
}

resource "aws_emr_cluster" "spark" {
  name          = "${var.name_prefix}-emr"
  release_label = var.release_label
  applications  = ["Spark", "Hive", "JupyterEnterpriseGateway"]

  log_uri = "s3://${var.lakehouse_bucket}/emr-logs/"

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    bid_price      = var.core_spot_bid_price

    ebs_config {
      size                 = var.ebs_volume_size_gb
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  auto_termination_policy {
    idle_timeout = var.idle_timeout_seconds
  }

  configurations_json = local.configurations

  bootstrap_action {
    path = "s3://${var.bootstrap_bucket}/${var.bootstrap_object_key}"
    name = "install-pulsetrack-deps"
    args = [var.bootstrap_bucket]
  }

  ec2_attributes {
    instance_profile                  = var.instance_profile_arn
    subnet_id                         = var.subnet_id
    key_name                          = var.key_pair_name
    emr_managed_master_security_group = var.emr_security_group_id
    emr_managed_slave_security_group  = var.emr_security_group_id
    service_access_security_group     = var.service_security_group_id
  }

  service_role = var.service_role_arn

  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true
  visible_to_all_users              = true

  tags = {
    Name = "${var.name_prefix}-emr"
  }

  lifecycle {
    ignore_changes = [step]
  }
}
