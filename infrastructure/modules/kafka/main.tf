resource "aws_msk_serverless_cluster" "pulsetrack" {
  cluster_name = "${var.name_prefix}-msk"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [var.kafka_security_group_id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Name = "${var.name_prefix}-msk"
  }
}
