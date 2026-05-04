# Dev environment — small instances, spot pricing, aggressive auto-terminate.
environment = "dev"

emr_release_label        = "emr-7.2.0"
emr_master_instance_type = "m5.xlarge"
emr_core_instance_type   = "m5.xlarge"
emr_core_instance_count  = 2
emr_core_spot_bid_price  = "0.08"
emr_ebs_volume_size_gb   = 64
emr_idle_timeout_seconds = 7200 # 2h auto-terminate

budget_limit_usd = "40"
