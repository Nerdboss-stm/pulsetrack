output "cluster_id" {
  value = aws_emr_cluster.spark.id
}

output "cluster_name" {
  value = aws_emr_cluster.spark.name
}

output "master_public_dns" {
  value = aws_emr_cluster.spark.master_public_dns
}
