
resource "aws_s3_bucket" "lambda" {
  bucket = "temp-bucket"
  acl    = "private"
}

resource "aws_s3_object" "ingester_zip" {
  bucket = aws_s3_bucket.lambda.bucket
  key    = "ingester.zip"
  source = "${path.module}/zips/ingester.zip" 
  acl    = "private"
}

resource "aws_s3_object" "processor_zip" {
  bucket = aws_s3_bucket.lambda.bucket
  key    = "processor.zip"
  source = "${path.module}/zips/processor.zip" 
  acl    = "private"
}