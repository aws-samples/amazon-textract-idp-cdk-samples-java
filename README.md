# Welcome to your CDK Java project!

This is a blank project for CDK development with Java.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Useful commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!

# Workflow

Deploy using
```bash
cdk deploy DocumentSplitterWorkflow
```

This samples includes a new component called DocumentSpliter, which takes and input document of type TIFF or PDF and outputs each individual page to an S3 location and adds the list of filenames to an array. 

That array is then used in a [Step Functions Map state](https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html) and processed in parallel. Each iteration classifies the page and then in case of a W2 or paystub routes to an extraction process or not. At the end all the W2s and Paystubs are extracted and the map returns and array with the page numbers and their classification result.

<img alt="DocumentSplitter" width="400px" src="https://amazon-textract-public-content.s3.us-east-2.amazonaws.com/idp-cdk-samples/documentation/IDP_CDK_Samples_DocumentSplitter_Java.svg" />

Test with a sample document using:

```bash
 aws s3 cp s3://amazon-textract-public-content/idp-cdk-samples/moby-dick-hidden-paystub-and-w2.pdf $(aws cloudformation list-exports --query 'Exports[?Name==`DocumentSplitterWorkflow-DocumentUploadLocation`].Value' --output text) 
```