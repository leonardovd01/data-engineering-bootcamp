resource "aws_s3_bucket" "bucket" {
  bucket_prefix = var.bucket_prefix

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "s3-data-bootcamp-leovd912823"
  }
}

resource "aws_s3_bucket_object" "object_movie" {
  for_each = fileset("/home/leo/LeoBootCamp/data/", "**")
  bucket   = aws_s3_bucket.bucket.id
  key      = each.value
  source   = "/home/leo/LeoBootCamp/data/${each.value}"
  etag     = filemd5("/home/leo/LeoBootCamp/data/${each.value}")
}

#/home/leo/LeoBootCamp
#resource "aws_s3_bucket_object" "object_log" {
#  bucket   = aws_s3_bucket.bucket.id
#  key      = "log_reviews.csv"
#  source   = "data/log_reviews.csv"
#  etag     = filemd5("data/log_reviews.csv")
#}
#resource "aws_s3_bucket_object" "object_purchase" {
#  bucket   = aws_s3_bucket.bucket.id
#  key      = "user_purchase.csv"
#  source   = "data/user_purchase.csv"
#  etag     = filemd5("data/user_purchase.csv")
#}
