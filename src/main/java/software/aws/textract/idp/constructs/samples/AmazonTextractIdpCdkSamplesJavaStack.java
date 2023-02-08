package software.aws.textract.idp.constructs.samples;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.RemovalPolicy;

import java.util.HashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.Aws;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.EventType;
import software.amazon.awscdk.services.s3.notifications.LambdaDestination;
import software.amazon.awscdk.services.s3.NotificationKeyFilter;
import software.amazon.textract.idp.DocumentSplitter;
import software.amazon.textract.idp.SpacySfnTask;
import software.amazon.textract.idp.TextractClassificationConfigurator;
import software.amazon.textract.idp.TextractGenerateCSV;
import software.amazon.textract.idp.TextractGenericSyncSfnTask;
import software.amazon.textract.idp.TextractPOCDecider;
import software.amazon.awscdk.services.stepfunctions.Map;
import software.amazon.awscdk.services.stepfunctions.JsonPath;
import software.amazon.awscdk.services.stepfunctions.Chain;
import software.amazon.awscdk.services.stepfunctions.Choice;
import software.amazon.awscdk.services.stepfunctions.Condition;
import software.amazon.awscdk.services.stepfunctions.tasks.LambdaInvoke;
import software.amazon.awscdk.services.stepfunctions.IntegrationPattern;
import software.amazon.awscdk.services.stepfunctions.StateMachine;
import software.amazon.awscdk.services.stepfunctions.TaskInput;

public class AmazonTextractIdpCdkSamplesJavaStack extends Stack {
    public AmazonTextractIdpCdkSamplesJavaStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public AmazonTextractIdpCdkSamplesJavaStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // The code that defines your stack goes here
        String s3UploadPrefix = "uploads";
        String s3OutputPrefix = "textract-output";
        String s3TxtOutputPrefix = "textract-text-output";
        String s3CsvOutputPrefix = "textract-csv-output";

        final Bucket bucket = Bucket.Builder.create(this, "DocumentSplitterJava")
                .autoDeleteObjects(true)
                .removalPolicy(RemovalPolicy.DESTROY)
                .build();

        final TextractPOCDecider deciderTask = TextractPOCDecider.Builder.create(this, "Decider").build();

        DocumentSplitter documentSplitterTask = DocumentSplitter.Builder.create(this, "DocumentSplitter")
                .s3OutputBucket(bucket.getBucketName())
                .s3OutputPrefix(s3OutputPrefix)
                .build();

        TextractGenericSyncSfnTask textractSyncTask = TextractGenericSyncSfnTask.Builder.create(this, "TextractSync")
                .s3OutputBucket(bucket.getBucketName())
                .s3OutputPrefix(s3OutputPrefix)
                .integrationPattern(IntegrationPattern.WAIT_FOR_TASK_TOKEN)
                .lambdaLogLevel("DEBUG")
                .timeout(Duration.hours(24))
                .input(TaskInput.fromObject(java.util.Map.of("Token", JsonPath.getTaskToken(),
                        "ExecutionId", JsonPath.stringAt("$$.Execution.Id"),
                        "Payload", JsonPath.getEntirePayload())))
                .resultPath("$.textract_result")
                .build();

        TextractGenerateCSV generateText = TextractGenerateCSV.Builder.create(this, "GenerateCSV")
                .csvS3OutputBucket(bucket.getBucketName())
                .csvS3OutputPrefix(s3TxtOutputPrefix)
                .outputType("LINES")
                .lambdaLogLevel("DEBUG")
                .integrationPattern(IntegrationPattern.WAIT_FOR_TASK_TOKEN)
                .input(TaskInput.fromObject(java.util.Map.of("Token", JsonPath.getTaskToken(),
                        "ExecutionId", JsonPath.stringAt("$$.Execution.Id"),
                        "Payload", JsonPath.getEntirePayload())))
                .resultPath("$.txt_output_location")
                .build();

        SpacySfnTask spacyClassification = SpacySfnTask.Builder.create(this, "Classification")
                .lambdaLogLevel("DEBUG")
                .integrationPattern(IntegrationPattern.WAIT_FOR_TASK_TOKEN)
                .input(TaskInput.fromObject(java.util.Map.of("Token", JsonPath.getTaskToken(),
                        "ExecutionId", JsonPath.stringAt("$$.Execution.Id"),
                        "Payload", JsonPath.getEntirePayload())))
                .resultPath("$.classification")
                .build();

        TextractClassificationConfigurator configuratorTask = TextractClassificationConfigurator.Builder
                .create(this, "Configurator").build();

        TextractGenericSyncSfnTask textractQueriesSyncTask = TextractGenericSyncSfnTask.Builder
                .create(this, "TextractQueriesSync")
                .s3OutputBucket(bucket.getBucketName())
                .s3OutputPrefix(s3OutputPrefix)
                .integrationPattern(IntegrationPattern.WAIT_FOR_TASK_TOKEN)
                .lambdaLogLevel("DEBUG")
                .timeout(Duration.hours(24))
                .input(TaskInput.fromObject(java.util.Map.of("Token", JsonPath.getTaskToken(),
                        "ExecutionId", JsonPath.stringAt("$$.Execution.Id"),
                        "Payload", JsonPath.getEntirePayload())))
                .resultPath("$.textract_result")
                .build();

        TextractGenerateCSV generateCSV = TextractGenerateCSV.Builder.create(this, "GenerateText")
                .csvS3OutputBucket(bucket.getBucketName())
                .csvS3OutputPrefix(s3CsvOutputPrefix)
                .outputType("CSV")
                .lambdaLogLevel("DEBUG")
                .integrationPattern(IntegrationPattern.WAIT_FOR_TASK_TOKEN)
                .input(TaskInput.fromObject(java.util.Map.of("Token", JsonPath.getTaskToken(),
                        "ExecutionId", JsonPath.stringAt("$$.Execution.Id"),
                        "Payload", JsonPath.getEntirePayload())))
                .resultPath("$.csv_output_location")
                .build();

        DockerImageFunction lambdaGenerateClassificationMapping = DockerImageFunction.Builder
                .create(this, "LambdaGenerateClassificationMapping")
                .code(DockerImageCode.fromImageAsset("lambda/map_classifications_lambda/"))
                .memorySize(128)
                .architecture(Architecture.X86_64)
                .build();

        LambdaInvoke taskGenerateClassificationMapping = LambdaInvoke.Builder
                .create(this, "TaskGenerate ClassificationMapping")
                .lambdaFunction(lambdaGenerateClassificationMapping)
                .outputPath("$.Payload")
                .build();

        /** CREATE WORKFLOW */
        Choice docTypeChoice = Choice.Builder.create(this, "RouteDocType")
                .build()
                .when(Condition.stringEquals("$.classification.documentType", "NONE"),
                        taskGenerateClassificationMapping)
                .when(Condition.stringEquals("$.classification.documentType", "AWS_OTHER"),
                        taskGenerateClassificationMapping)
                .otherwise(configuratorTask);

        String mapParameters = "{\"manifest\":{\"s3Path.$\":\"States.Format('s3://{}/{}/{}', $.documentSplitterS3OutputBucket, $.documentSplitterS3OutputPath, $$.Map.Item.Value)\"},\"mime.$\":\"$.mime\",\"numberOfPages\":1}";

        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };

        HashMap<String, Object> o = new HashMap<>();
        try {
            o = new ObjectMapper().readValue(mapParameters, typeRef);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        // java.util.Map<String, Object> mapParametersObject = new
        // ObjectMapper().readValue(mapParameters, new
        // TypeReference<HashMap<String,Object>>() {});

        Map map = Map.Builder.create(this, "Map")
                .itemsPath(JsonPath.stringAt("$.pages"))
                .parameters(o)
                .build();

        textractSyncTask.next(generateText).next(spacyClassification).next(docTypeChoice);

        configuratorTask.next(textractQueriesSyncTask).next(generateCSV).next(taskGenerateClassificationMapping);

        map.iterator(textractSyncTask);

        Chain workflowChain = Chain.start(deciderTask).next(documentSplitterTask).next(map);

        StateMachine stateMachine = StateMachine.Builder.create(this, "ParentStateMachine")
                .definition(workflowChain)
                .build();

        // ############## S3 event trigger to Lambda for kicking off workflow
        DockerImageFunction lambdaStartStepFunction = DockerImageFunction.Builder
                .create(this, "LambdaStartStepFunction")
                .code(DockerImageCode.fromImageAsset("lambda/startstepfunction"))
                .memorySize(128)
                .architecture(Architecture.X86_64)
                .environment(java.util.Map.of("STATE_MACHINE_ARN", stateMachine.getStateMachineArn()))
                .build();

        lambdaStartStepFunction.addToRolePolicy(PolicyStatement.Builder.create()
                .actions(java.util.List.of("states:StartExecution"))
                .resources(java.util.List.of(stateMachine.getStateMachineArn()))
                .build());

        bucket.addEventNotification(EventType.OBJECT_CREATED,
                new LambdaDestination(lambdaStartStepFunction),
                NotificationKeyFilter.builder().prefix(s3UploadPrefix).build());

        // OUTPUT
        CfnOutput.Builder.create(this, "DocumentUploadLocation")
                .value("s3://" + bucket.getBucketName() + "/" + s3UploadPrefix)
                .exportName(Aws.STACK_NAME+"-DocumentUploadLocation")
                .build();
        String currentRegion = Stack.of(this).getRegion();
        CfnOutput.Builder.create(this, "StepFunctionFlowLink")
                .value("https://" + currentRegion
                        + ".console.aws.amazon.com/states/home?region=" + currentRegion + "#/statemachines/view/"
                        + stateMachine.getStateMachineArn())
                .exportName(Aws.STACK_NAME+"-StepFunctionFlowLink")
                .build();
    }
}
