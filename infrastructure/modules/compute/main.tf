locals {
  configurations = templatefile("${path.module}/configurations.json", {
    lakehouse_bucket = var.lakehouse_bucket
  })
}

resource "aws_emr_cluster" "spark" {
  name          = "${var.name_prefix}-emr"
  release_label = var.release_label
  # ``applications`` is the list of EMR-managed services to install on the
  # cluster. Each entry MUST be a name EMR recognizes — the
  # ``RunJobFlow`` API rejects unknowns with
  # ``ValidationException: Specified application: X is invalid``.
  #
  # Iceberg and Delta are NOT managed EMR applications; both are bundled
  # libraries:
  #   * Iceberg: jars at ``/usr/share/aws/iceberg/lib/``; EMR auto-symlinks
  #     into ``/usr/lib/spark/jars/`` since release 6.5.0. Spark sees the
  #     classes; the ``glue_iceberg`` catalog and SQL extensions are wired
  #     in ``configurations.json`` (template below).
  #   * Delta: jars at ``/usr/share/aws/delta/lib/`` (newer EMR releases),
  #     NOT auto-symlinked. ``bootstrap.sh`` does the symlink in a
  #     version-glob loop so the bootstrap survives release-label bumps.
  #
  # Adding "Iceberg" or "Delta" here would fail RunJobFlow validation.
  applications = ["Spark", "Hive", "JupyterEnterpriseGateway"]

  log_uri = "s3://${var.lakehouse_bucket}/emr-logs/"

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    # Empty string => on-demand (passing null to bid_price makes EMR use the
    # ON_DEMAND market). Streaming workloads can't tolerate spot reclamation
    # mid-run — see postmortems/2026-05-11_emr_cluster_bringup_13_incidents.md
    bid_price = var.core_spot_bid_price == "" ? null : var.core_spot_bid_price

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
    # service_access_security_group is forbidden for clusters launched in a public subnet.
    # AWS validation: "You cannot specify a ServiceAccessSecurityGroup for a cluster launched in public subnet."
    # We run EMR in the public subnet (no NAT, no private subnet by design — keeps cost at $0/idle).
  }

  service_role = var.service_role_arn

  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true
  visible_to_all_users              = true

  # Step concurrency = how many steps run in parallel. Default 1 is FATAL
  # for our pipeline (bronze + silver + gold facts = 4 streaming queries
  # MUST run concurrently). With concurrency=1 they queue serially and
  # the "wait for streams ACTIVE" loop in scripts/run_scale_test.sh times
  # out. Bumped to 10 to absorb the 4 streams + 4 dim pre-builds + the
  # batch tier (EHR/pharmacy silver, identity_bridge, dim_patient) without
  # queueing. Reference: ADR-008, plus the live-cluster modify-cluster
  # we ran during the gap-closure run because the running cluster had
  # default concurrency 1.
  step_concurrency_level = 10

  tags = {
    Name = "${var.name_prefix}-emr"
  }

  lifecycle {
    ignore_changes = [step]
  }
}
