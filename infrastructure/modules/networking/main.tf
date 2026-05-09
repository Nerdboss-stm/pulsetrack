data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_subnet" "public" {
  count                   = length(var.public_subnet_cidrs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.name_prefix}-public-${count.index}"
    Tier = "public"
    # Required by AmazonEMRServicePolicy_v2 — without this tag the v2
    # service role can't configure EMR rules on the subnet.
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.name_prefix}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# S3 gateway endpoint — keeps S3 traffic inside AWS, no per-GB charges
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = { Name = "${var.name_prefix}-s3-endpoint" }
}

# ─── Security groups ────────────────────────────────────────────────────────

resource "aws_security_group" "emr_master" {
  name        = "${var.name_prefix}-emr-master"
  description = "EMR master node - SSH and Spark UI"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # EMR Block Public Access (account-level, on by default) rejects any non-SSH
  # ingress to 0.0.0.0/0 on the master SG. Access Spark UI (8088) and History
  # Server (18080) via SSH tunnel instead:
  #   ssh -i pulsetrack-emr.pem -L 8088:localhost:8088 -L 18080:localhost:18080 hadoop@<master-dns>

  egress {
    description = "All egress"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-emr-master"
    # Required by AmazonEMRServicePolicy_v2 — EMR adds inter-node rules at
    # cluster launch and the v2 policy gates that on this tag.
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
}

resource "aws_security_group" "emr_service" {
  name        = "${var.name_prefix}-emr-service"
  description = "EMR service access - internal control plane"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-emr-service"
    # Required by AmazonEMRServicePolicy_v2 (kept for parity even though we
    # don't pass this SG to a public-subnet cluster).
    "for-use-with-amazon-emr-managed-policies" = "true"
  }
}

# Intra-cluster traffic for EMR master/core
resource "aws_security_group_rule" "emr_master_self" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.emr_master.id
  description       = "All intra-cluster traffic"
}

resource "aws_security_group" "kafka" {
  name        = "${var.name_prefix}-kafka"
  description = "MSK Serverless - TLS/SASL clients"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Kafka TLS from EMR"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.emr_master.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.name_prefix}-kafka" }
}
