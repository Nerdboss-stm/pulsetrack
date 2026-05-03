output "vpc_id" {
  value = aws_vpc.main.id
}

output "public_subnet_ids" {
  value = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "emr_security_group_id" {
  value = aws_security_group.emr_master.id
}

output "emr_service_security_group_id" {
  value = aws_security_group.emr_service.id
}

output "kafka_security_group_id" {
  value = aws_security_group.kafka.id
}
