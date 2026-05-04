# Prod environment — TEMPLATE ONLY. We only run dev.
# Do NOT apply this without revisiting the cost and security implications.
environment = "prod"

emr_release_label        = "emr-7.2.0"
emr_master_instance_type = "m5.2xlarge"
emr_core_instance_type   = "m5.2xlarge"
emr_core_instance_count  = 4
emr_core_spot_bid_price  = "" # disable spot in prod (on-demand)
emr_ebs_volume_size_gb   = 256
emr_idle_timeout_seconds = 86400 # 24h

budget_limit_usd = "500"
