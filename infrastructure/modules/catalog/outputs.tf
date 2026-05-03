output "bronze_database_name" {
  value = aws_glue_catalog_database.bronze.name
}

output "silver_database_name" {
  value = aws_glue_catalog_database.silver.name
}

output "gold_database_name" {
  value = aws_glue_catalog_database.gold.name
}

output "bronze_crawler_name" {
  value = aws_glue_crawler.bronze.name
}

output "silver_crawler_name" {
  value = aws_glue_crawler.silver.name
}

output "gold_crawler_name" {
  value = aws_glue_crawler.gold.name
}
