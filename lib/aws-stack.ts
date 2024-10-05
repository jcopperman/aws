import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as apigw from 'aws-cdk-lib/aws-apigateway';

export class AwsStack  extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create S3 buckets
    const inputBucket = new s3.Bucket(this, 'InputBucket');
    const outputBucket = new s3.Bucket(this, 'OutputBucket');

    // Create SQS Queue
    const queue = new sqs.Queue(this, 'AnonymizerQueue');

    // Layer for dependencies
    const layerCode = lambda.Code.fromAsset('lambda_layers/python');
    const processingLambdaLayer = new lambda.LayerVersion(this, 'ProcessingLambdaLayer', {
      code: layerCode,
    });

    // Lambda Function 1: Request Handler
    const lambda1 = new lambda.Function(this, 'RequestHandler', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'request_handler.handler',
      code: lambda.Code.fromAsset('lambda_functions/request_handler'),
      environment: {
        QUEUE_URL: queue.queueUrl
      },
      layers: [processingLambdaLayer]
    });

    // Lambda Function 2: Processing Lambda
    const lambda2 = new lambda.Function(this, 'ProcessingLambda', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'processing_lambda.handler',
      code: lambda.Code.fromAsset('lambda_functions/processing_lambda'),
      environment: {
        OUTPUT_BUCKET_NAME: outputBucket.bucketName
      },
      layers: [processingLambdaLayer]
    });

    // Lambda Function 3: Status Checker
    const lambda3 = new lambda.Function(this, 'StatusChecker', {
      runtime: lambda.Runtime.PYTHON_3_8,
      handler: 'status_checker.handler',
      code: lambda.Code.fromAsset('lambda_functions/status_checker'),
      environment: {
        OUTPUT_BUCKET_NAME: outputBucket.bucketName
      },
      layers: [processingLambdaLayer]
    });

    // API Gateway Setup
    const api = new apigw.RestApi(this, 'DataAnonymizerAPI');

    // Upload endpoint
    const uploadResource = api.root.addResource('upload');
    const uploadIntegration = new apigw.LambdaIntegration(lambda1);
    uploadResource.addMethod('POST', uploadIntegration);

    // Status check endpoint
    const statusResource = api.root.addResource('status').addResource('{fileId}');
    const statusIntegration = new apigw.LambdaIntegration(lambda3);
    statusResource.addMethod('GET', statusIntegration);
  }
}
