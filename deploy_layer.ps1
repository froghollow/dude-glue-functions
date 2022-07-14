$layername = "dtl-dev-glue_functions"
$layernum = "1"
$zipFile = "glue_functions_1.zip"
$stack = 'dtl-prd-FP4'

cd C:\Users\Public\Code\dude-file-processing-4\common\glue_functions

Compress-Archive -Path .\python -DestinationPath $zipFile -Force

aws s3 cp $zipFile s3://wc2h-dtl-dev-code/lambda/$zipFile
aws lambda publish-layer-version --layer-name $layername --content S3Bucket=wc2h-dtl-dev-code,S3Key=lambda/$zipFile

$lambda_functions = (
  'Tableau_S3_Convert'
)

ForEach( $function_name in $lambda_functions ) {
    Write-Output $stack-$function_name
    aws lambda update-function-configuration --function-name $stack-$function_name --layers arn:aws-us-gov:lambda:us-gov-west-1:783303832209:layer:dtl-dev-tableau_functions:$layernum
}
