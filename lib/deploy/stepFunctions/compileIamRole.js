'use strict';

const _ = require('lodash');
const BbPromise = require('bluebird');
const path = require('path');
const { isIntrinsic, translateLocalFunctionNames, trimAliasFromLambdaArn } = require('../../utils/aws');
const { getArnPartition } = require('../../utils/arn');

const logger = require('../../utils/logger');

function getTaskStates(states, stateMachineName) {
  return _.flatMap(states, (state) => {
    switch (state.Type) {
      case 'Task': {
        return [state];
      }
      case 'Parallel': {
        const parallelStates = _.flatMap(state.Branches, branch => _.values(branch.States));
        return getTaskStates(parallelStates, stateMachineName);
      }
      case 'Map': {
        const mapStates = state.ItemProcessor ? state.ItemProcessor.States : state.Iterator.States;
        const taskStates = getTaskStates(mapStates, stateMachineName);
        if (state.ItemProcessor && state.ItemProcessor.ProcessorConfig
          && state.ItemProcessor.ProcessorConfig.Mode === 'DISTRIBUTED') {
          taskStates.push({
            Resource: 'arn:aws:states:::states:startExecution',
            Mode: 'DISTRIBUTED',
            StateMachineName: stateMachineName,
          });
        }
        if (state.ItemReader) {
          taskStates.push(state.ItemReader);
        }
        if (state.ResultWriter) {
          taskStates.push(state.ResultWriter);
        }
        return taskStates;
      }
      default: {
        return [];
      }
    }
  });
}

function sqsQueueUrlToArn(serverless, queueUrl) {
  const regex = /https:\/\/sqs.(.*).amazonaws.com\/(.*)\/(.*)/g;
  const match = regex.exec(queueUrl);
  if (match) {
    const region = match[1];
    const accountId = match[2];
    const queueName = match[3];
    const partition = getArnPartition(region);
    return `arn:${partition}:sqs:${region}:${accountId}:${queueName}`;
  }
  if (isIntrinsic(queueUrl)) {
    if (queueUrl.Ref) {
      // most likely we'll see a { Ref: LogicalId }, which we need to map to
      // { Fn::GetAtt: [ LogicalId, Arn ] } to get the ARN
      return {
        'Fn::GetAtt': [queueUrl.Ref, 'Arn'],
      };
    }
    // in case of for example { Fn::ImportValue: sharedValueToImport }
    // we need to use "*" as ARN
    return '*';
  }
  logger.log(`Unable to parse SQS queue url [${queueUrl}]`);
  return [];
}

function getParameters(state) {
  // Handle different ways parameters can be expressed in state machine tasks
  if (state.QueryLanguage === 'JSONata') {
    // For JSONata query language, return the Arguments object
    return state.Arguments;
  }
  if (state.Parameters) {
    // For standard task states with Parameters
    return state.Parameters;
  }
  if (state.Arguments) {
    // For task states using the Arguments field (newer syntax)
    return state.Arguments;
  }
  if (state.ItemReader || state.ItemProcessor || state.ResultWriter) {
    // For Map states with specialized components
    return state.ItemReader || state.ItemProcessor || state.ResultWriter;
  }
  // Default case: return empty object to prevent null reference errors
  return {};
}

function getSqsPermissions(serverless, state) {
  const parameters = getParameters(state);
  if (_.has(parameters, 'QueueUrl') || _.has(parameters, 'QueueUrl.$')) {
    const queueArn = parameters['QueueUrl.$'] ? '*' : sqsQueueUrlToArn(serverless, parameters.QueueUrl);
    return [{ action: 'sqs:SendMessage', resource: queueArn }];
  }
  logger.log('SQS task missing Parameters.QueueUrl or Parameters.QueueUrl.$');
  return [];
}

function getSnsPermissions(serverless, state) {
  const parameters = getParameters(state);
  if (_.has(parameters, 'TopicArn') || _.has(parameters, 'TopicArn.$')) {
    const topicArn = parameters['TopicArn.$'] ? '*' : parameters.TopicArn;
    return [{ action: 'sns:Publish', resource: topicArn }];
  }
  logger.log('SNS task missing Parameters.TopicArn or Parameters.TopicArn.$');
  return [];
}

function getDynamoDBArn(tableName) {
  if (isIntrinsic(tableName)) {
    // most likely we'll see a { Ref: LogicalId }, which we need to map to
    // { Fn::GetAtt: [ LogicalId, Arn ] } to get the ARN
    if (tableName.Ref) {
      return {
        'Fn::GetAtt': [tableName.Ref, 'Arn'],
      };
    }
    // but also support importing the table name from an external stack that exports it
    // as we still want to support direct state machine actions interacting with those tables
    if (tableName['Fn::ImportValue']) {
      return {
        'Fn::Join': [
          ':',
          [
            'arn',
            { Ref: 'AWS::Partition' },
            'dynamodb',
            { Ref: 'AWS::Region' },
            { Ref: 'AWS::AccountId' },
            {
              'Fn::Join': [
                '/',
                [
                  'table',
                  tableName,
                ],
              ],
            },
          ],
        ],
      };
    }
  }

  return {
    'Fn::Join': [
      ':',
      [
        'arn',
        { Ref: 'AWS::Partition' },
        'dynamodb',
        { Ref: 'AWS::Region' },
        { Ref: 'AWS::AccountId' },
        `table/${tableName}`,
      ],
    ],
  };
}

function getBatchPermissions() {
  return [{
    action: 'batch:SubmitJob,batch:DescribeJobs,batch:TerminateJob',
    resource: '*',
  }, {
    action: 'events:PutTargets,events:PutRule,events:DescribeRule',
    resource: {
      'Fn::Join': [
        ':',
        [
          'arn',
          { Ref: 'AWS::Partition' },
          'events',
          { Ref: 'AWS::Region' },
          { Ref: 'AWS::AccountId' },
          'rule/StepFunctionsGetEventsForBatchJobsRule',
        ],
      ],
    },
  }];
}

function getGluePermissions() {
  return [{
    action: 'glue:StartJobRun,glue:GetJobRun,glue:GetJobRuns,glue:BatchStopJobRun',
    resource: '*',
  }];
}

function getEcsPermissions() {
  return [{
    action: 'ecs:RunTask,ecs:StopTask,ecs:DescribeTasks,iam:PassRole',
    resource: '*',
  }, {
    action: 'events:PutTargets,events:PutRule,events:DescribeRule',
    resource: {
      'Fn::Join': [
        ':',
        [
          'arn',
          { Ref: 'AWS::Partition' },
          'events',
          { Ref: 'AWS::Region' },
          { Ref: 'AWS::AccountId' },
          'rule/StepFunctionsGetEventsForECSTaskRule',
        ],
      ],
    },
  }];
}

function getDynamoDBPermissions(action, state) {
  const parameters = getParameters(state);
  let resource;

  if (parameters['TableName.$']) {
    // When the TableName is only known at runtime, we
    // have to provide * permissions during deployment.
    resource = '*';
  } else if (parameters['IndexName.$'] || parameters.IndexName) {
    // When the Parameters contain an IndexName, we have to build a
    // longer arn that includes the index.
    const indexName = parameters['IndexName.$']
      // We must provide * here instead of parameters['IndexName.$'], because we don't know
      // which index will be targeted when we the step function runs
      ? '*'
      : parameters.IndexName;

    resource = getDynamoDBArn(`${parameters.TableName}/index/${indexName}`);
  } else {
    resource = getDynamoDBArn(parameters.TableName);
  }

  return [{
    action,
    resource,
  }];
}

function getBatchDynamoDBPermissions(action, state) {
  const parameters = getParameters(state);
  if (parameters['RequestItems.$']) {
    // When the RequestItems object is only known at runtime,
    // we have to provide * permissions during deployment.
    return [{
      action,
      resource: '*',
    }];
  }
  // If RequestItems is specified it must contain the target
  // table names as keys. We can use these to generate roles
  // whether the array of requests for that table is known
  // at deploy time or not
  const tableNames = Object.keys(parameters.RequestItems);

  return tableNames.map(tableName => ({
    action,
    resource: getDynamoDBArn(tableName.replace('.$', '')),
  }));
}

function getRedshiftDataPermissions(action, state) {
  const parameters = getParameters(state);
  const permissions = [];

  if (['redshift-data:ExecuteStatement', 'redshift-data:BatchExecuteStatement'].includes(action)) {
    const dbName = _.has(parameters, 'Database') ? parameters.Database : '*';

    let workgroupArn;
    let clusterName;
    if (_.has(parameters, 'WorkgroupName')) {
      if (parameters.WorkgroupName.startsWith('arn:')) {
        workgroupArn = parameters.WorkgroupName;
      } else {
        workgroupArn = { 'Fn::Sub': 'arn:${AWS::Partition}:redshift-serverless:${AWS::Region}:${AWS::AccountId}:workgroup/*' };
      }
    } else if (_.has(parameters, 'WorkgroupName.$')) {
      workgroupArn = { 'Fn::Sub': 'arn:${AWS::Partition}:redshift-serverless:${AWS::Region}:${AWS::AccountId}:workgroup/*' };
    } else if (_.has(parameters, 'ClusterIdentifier')) {
      clusterName = parameters.ClusterIdentifier;
    } else {
      clusterName = '*';
    }

    let secretArn;
    let dbUser;
    if (_.has(parameters, 'SecretArn')) {
      if (parameters.SecretArn.startsWith('arn:')) {
        secretArn = parameters.SecretArn;
      } else {
        secretArn = { 'Fn::Sub': `arn:\${AWS::Partition}:secretsmanager:\${AWS::Region}:\${AWS::AccountId}:secret:${parameters.SecretArn}*` };
      }
    } else if (_.has(parameters, 'SecretArn.$')) {
      secretArn = { 'Fn::Sub': 'arn:${AWS::Partition}:secretsmanager:${AWS::Region}:${AWS::AccountId}:secret:*' };
    } else if (_.has(parameters, 'DbUser')) {
      dbUser = parameters.DbUser;
    } else if (_.has(parameters, 'DbUser.$')) {
      dbUser = '*';
    }

    permissions.push({
      action,
      resource: workgroupArn || { 'Fn::Sub': `arn:\${AWS::Partition}:redshift:\${AWS::Region}:\${AWS::AccountId}:cluster:${clusterName}` },
    });

    if (secretArn) {
      permissions.push({
        action: 'secretsmanager:GetSecretValue',
        resource: secretArn,
      });
    } else if (dbUser) {
      permissions.push({
        action: 'redshift:GetClusterCredentials',
        resource: [
          { 'Fn::Sub': `arn:\${AWS::Partition}:redshift:\${AWS::Region}:\${AWS::AccountId}:dbuser:${clusterName}/${dbUser}` },
          { 'Fn::Sub': `arn:\${AWS::Partition}:redshift:\${AWS::Region}:\${AWS::AccountId}:dbname:${clusterName}/${dbName}` },
        ],
      });
    } else {
      if (workgroupArn) { // eslint-disable-line no-lonely-if
        permissions.push({
          action: 'redshift-serverless:GetCredentials',
          resource: workgroupArn,
        });
      } else {
        permissions.push({
          action: 'redshift:GetClusterCredentialsWithIAM',
          resource: { 'Fn::Sub': `arn:\${AWS::Partition}:redshift:\${AWS::Region}:\${AWS::AccountId}:dbname:${clusterName}/${dbName}` },
        });
      }
    }
  } else {
    permissions.push({
      action,
      resource: '*',
    });
  }

  return permissions;
}

function getLambdaPermissions(state) {
  const parameters = getParameters(state);
  const functionName = parameters.FunctionName;
  if (_.isString(functionName)) {
    const segments = functionName.split(':');

    let functionArns;
    if (functionName.match(/^arn:aws(-[a-z]+)*:lambda/)) {
      // full ARN
      functionArns = [
        functionName,
        `${functionName}:*`,
      ];
    } else if (segments.length === 3 && segments[0].match(/^\d+$/)) {
      // partial ARN
      functionArns = [
        { 'Fn::Sub': `arn:\${AWS::Partition}:lambda:\${AWS::Region}:${functionName}` },
        { 'Fn::Sub': `arn:\${AWS::Partition}:lambda:\${AWS::Region}:${functionName}:*` },
      ];
    } else {
      // name-only (with or without alias)
      functionArns = [
        {
          'Fn::Sub': `arn:\${AWS::Partition}:lambda:\${AWS::Region}:\${AWS::AccountId}:function:${functionName}`,
        },
        {
          'Fn::Sub': `arn:\${AWS::Partition}:lambda:\${AWS::Region}:\${AWS::AccountId}:function:${functionName}:*`,
        },
      ];
    }

    return [{
      action: 'lambda:InvokeFunction',
      resource: functionArns,
    }];
  } if (_.has(functionName, 'Fn::GetAtt')) {
    const functionArn = translateLocalFunctionNames.bind(this)(functionName);
    return [{
      action: 'lambda:InvokeFunction',
      resource: [
        functionArn,
        { 'Fn::Sub': ['${functionArn}:*', { functionArn }] },
      ],
    }];
  } if (_.has(functionName, 'Ref')) {
    const functionArn = translateLocalFunctionNames.bind(this)(functionName);
    return [{
      action: 'lambda:InvokeFunction',
      resource: [
        {
          'Fn::Sub': [
            'arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${functionArn}',
            { functionArn },
          ],
        },
        {
          'Fn::Sub': [
            'arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${functionArn}:*',
            { functionArn },
          ],
        },
      ],
    }];
  }

  if (parameters['FunctionName.$']) {
    return [{
      action: 'lambda:InvokeFunction',
      resource: parameters.AllowedFunctions ? parameters.AllowedFunctions : '*',
    }];
  }

  return [{
    action: 'lambda:InvokeFunction',
    resource: functionName,
  }];
}

function getStateMachineArn(state) {
  let stateMachineArn;

  if (state.Arguments) {
    stateMachineArn = state.Arguments.StateMachineArn.trim().startsWith('{%')
      ? '*'
      : state.Arguments.StateMachineArn;
  } else {
    stateMachineArn = state.Parameters['StateMachineArn.$']
      ? '*'
      : state.Parameters.StateMachineArn;
  }

  return stateMachineArn;
}

function getStepFunctionsPermissions(state) {
  let stateMachineArn = state.Mode === 'DISTRIBUTED' ? {
    'Fn::Sub': [
      `arn:aws:states:\${AWS::Region}:\${AWS::AccountId}:stateMachine:${state.StateMachineName}`,
      {},
    ],
  } : null;

  if (!stateMachineArn) {
    stateMachineArn = getStateMachineArn(state);
  }

  return [{
    action: 'states:StartExecution',
    resource: stateMachineArn,
  }, {
    action: 'states:DescribeExecution,states:StopExecution',
    resource: '*',
  }, {
    action: 'events:PutTargets,events:PutRule,events:DescribeRule',
    resource: {
      'Fn::Sub': [
        'arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule',
        {},
      ],
    },
  }];
}

function getStepFunctionsSDKPermissions(state) {
  let stateMachineArn = state.Mode === 'DISTRIBUTED' ? {
    'Fn::Sub': [
      `arn:aws:states:\${AWS::Region}:\${AWS::AccountId}:stateMachine:${state.StateMachineName}`,
      {},
    ],
  } : null;

  if (!stateMachineArn) {
    stateMachineArn = getStateMachineArn(state);
  }

  return [{
    action: 'states:StartSyncExecution',
    resource: stateMachineArn,
  }, {
    action: 'states:DescribeExecution,states:StopExecution',
    resource: '*',
  }, {
    action: 'events:PutTargets,events:PutRule,events:DescribeRule',
    resource: {
      'Fn::Sub': [
        'arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule',
        {},
      ],
    },
  }];
}

function getCodeBuildPermissions(state) {
  const parameters = getParameters(state);
  const projectName = parameters.ProjectName;

  return [{
    action: 'codebuild:StartBuild,codebuild:StopBuild,codebuild:BatchGetBuilds',
    resource: {
      'Fn::Sub': [
        `arn:\${AWS::Partition}:codebuild:$\{AWS::Region}:$\{AWS::AccountId}:project/${projectName}`,
        {},
      ],
    },
  }, {
    action: 'events:PutTargets,events:PutRule,events:DescribeRule',
    resource: {
      'Fn::Sub': [
        'arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/StepFunctionsGetEventForCodeBuildStartBuildRule',
        {},
      ],
    },
  }];
}

function getSageMakerPermissions(state) {
  const parameters = getParameters(state);
  const transformJobName = parameters.TransformJobName ? `${parameters.TransformJobName}` : '';

  return [
    {
      action: 'sagemaker:CreateTransformJob,sagemaker:DescribeTransformJob,sagemaker:StopTransformJob',
      resource: {
        'Fn::Sub': [
          `arn:\${AWS::Partition}:sagemaker:$\{AWS::Region}:$\{AWS::AccountId}:transform-job/${transformJobName}*`,
          {},
        ],
      },
    },
    {
      action: 'sagemaker:ListTags',
      resource: '*',
    },
    {
      action: 'events:PutTargets,events:PutRule,events:DescribeRule',
      resource: {
        'Fn::Sub': [
          'arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:rule/StepFunctionsGetEventsForSageMakerTransformJobsRule',
          {},
        ],
      },
    },
  ];
}

function getBedrockPermissions(state) {
  const parameters = getParameters(state);
  const modelId = parameters.ModelId;
  const modelArn = modelId.startsWith('arn:') ? modelId : {
    'Fn::Sub': [
      `arn:\${AWS::Partition}:bedrock:$\{AWS::Region}::foundation-model/${modelId}`,
      {},
    ],
  };

  return [
    {
      action: 'bedrock:InvokeModel',
      resource: modelArn,
    },
  ];
}
function getEventBridgePermissions(state) {
  const parameters = getParameters(state);
  const eventBuses = new Set();
  for (const entry of parameters.Entries) {
    eventBuses.add(entry.EventBusName || 'default');
  }

  return [
    {
      action: 'events:PutEvents',
      resource: [...eventBuses].map(eventBus => ({
        'Fn::Sub': [
          'arn:${AWS::Partition}:events:${AWS::Region}:${AWS::AccountId}:event-bus/${eventBus}',
          { eventBus },
        ],
      })),
    },
  ];
}

function getEventBridgeSchedulerPermissions(state) {
  const parameters = getParameters(state);
  const scheduleGroupName = parameters.GroupName;
  const scheduleTargetRoleArn = parameters.Target.RoleArn;

  return [
    {
      action: 'scheduler:CreateSchedule',
      resource: {
        'Fn::Sub': [
          'arn:${AWS::Partition}:scheduler:${AWS::Region}:${AWS::AccountId}:schedule/${scheduleGroupName}/*',
          { scheduleGroupName },
        ],
      },
    },
    {
      action: 'iam:PassRole',
      resource: scheduleTargetRoleArn,
    },
  ];
}

function getS3ObjectPermissions(action, state) {
  const parameters = getParameters(state);
  const bucket = parameters.Bucket || '*';
  const key = parameters.Key || '*';
  const prefix = parameters.Prefix;
  let arn;

  if (action === 's3:listObjectsV2') {
    return [
      {
        action: 's3:Get*',
        resource: [
          `arn:aws:s3:::${bucket}`,
          `arn:aws:s3:::${bucket}/*`,
        ],
      },
      {
        action: 's3:List*',
        resource: [
          `arn:aws:s3:::${bucket}`,
          `arn:aws:s3:::${bucket}/*`,
        ],
      },
    ];
  }

  if (prefix) {
    arn = `arn:aws:s3:::${bucket}/${prefix}/${key}`;
  } else if (bucket === '*' && key === '*') {
    arn = '*';
  } else {
    arn = `arn:aws:s3:::${bucket}/${key}`;
  }

  return [{
    action,
    resource: [
      arn,
    ],
  }];
}

// if there are multiple permissions with the same action, then collapsed them into one
// permission instead, and collect the resources into an array
function consolidatePermissionsByAction(permissions) {
  return _.chain(permissions)
    .groupBy(perm => perm.action)
    .mapValues((perms) => {
      // find the unique resources
      let resources = _.uniqWith(_.flatMap(perms, p => p.resource), _.isEqual);
      if (_.includes(resources, '*')) {
        resources = '*';
      }

      return {
        action: perms[0].action,
        resource: resources,
      };
    })
    .values()
    .value(); // unchain
}

function consolidatePermissionsByResource(permissions) {
  return _.chain(permissions)
    .groupBy(p => JSON.stringify(p.resource))
    .mapValues((perms) => {
      // find unique actions
      const actions = _.uniq(_.flatMap(perms, p => p.action.split(',')));

      return {
        action: actions.join(','),
        resource: perms[0].resource,
      };
    })
    .values()
    .value(); // unchain
}

function getIamPermissions(taskStates) {
  return _.flatMap(taskStates, (state) => {
    const resourceName = typeof state.Resource === 'string' ? state.Resource.replace(/^arn:aws(-[a-z]+)*:/, 'arn:aws:') : state.Resource;
    switch (resourceName) {
      case 'arn:aws:states:::sqs:sendMessage':
      case 'arn:aws:states:::sqs:sendMessage.waitForTaskToken':
        return getSqsPermissions(this.serverless, state);

      case 'arn:aws:states:::sns:publish':
      case 'arn:aws:states:::sns:publish.waitForTaskToken':
        return getSnsPermissions(this.serverless, state);

      case 'arn:aws:states:::dynamodb:updateItem':
      case 'arn:aws:states:::aws-sdk:dynamodb:updateItem':
      case 'arn:aws:states:::aws-sdk:dynamodb:updateItem.waitForTaskToken':
        return getDynamoDBPermissions('dynamodb:UpdateItem', state);
      case 'arn:aws:states:::dynamodb:putItem':
      case 'arn:aws:states:::aws-sdk:dynamodb:putItem':
      case 'arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken':
        return getDynamoDBPermissions('dynamodb:PutItem', state);
      case 'arn:aws:states:::dynamodb:getItem':
        return getDynamoDBPermissions('dynamodb:GetItem', state);
      case 'arn:aws:states:::dynamodb:deleteItem':
        return getDynamoDBPermissions('dynamodb:DeleteItem', state);
      case 'arn:aws:states:::aws-sdk:dynamodb:updateTable':
        return getDynamoDBPermissions('dynamodb:UpdateTable', state);
      case 'arn:aws:states:::aws-sdk:dynamodb:query':
        return getDynamoDBPermissions('dynamodb:Query', state);

      case 'arn:aws:states:::aws-sdk:dynamodb:batchGetItem':
        return getBatchDynamoDBPermissions('dynamodb:BatchGetItem', state);
      case 'arn:aws:states:::aws-sdk:dynamodb:batchWriteItem':
        return getBatchDynamoDBPermissions('dynamodb:BatchWriteItem', state);

      case 'arn:aws:states:::aws-sdk:redshiftdata:executeStatement':
        return getRedshiftDataPermissions('redshift-data:ExecuteStatement', state);
      case 'arn:aws:states:::aws-sdk:redshiftdata:batchExecuteStatement':
        return getRedshiftDataPermissions('redshift-data:BatchExecuteStatement', state);
      case 'arn:aws:states:::aws-sdk:redshiftdata:listStatements':
        return getRedshiftDataPermissions('redshift-data:ListStatements', state);
      case 'arn:aws:states:::aws-sdk:redshiftdata:describeStatement':
        return getRedshiftDataPermissions('redshift-data:DescribeStatement', state);
      case 'arn:aws:states:::aws-sdk:redshiftdata:getStatementResult':
        return getRedshiftDataPermissions('redshift-data:GetStatementResult', state);
      case 'arn:aws:states:::aws-sdk:redshiftdata:cancelStatement':
        return getRedshiftDataPermissions('redshift-data:CancelStatement', state);

      case 'arn:aws:states:::batch:submitJob.sync':
      case 'arn:aws:states:::batch:submitJob':
        return getBatchPermissions();

      case 'arn:aws:states:::glue:startJobRun.sync':
      case 'arn:aws:states:::glue:startJobRun':
        return getGluePermissions();

      case 'arn:aws:states:::ecs:runTask.sync':
      case 'arn:aws:states:::ecs:runTask.waitForTaskToken':
      case 'arn:aws:states:::ecs:runTask':
        return getEcsPermissions();

      case 'arn:aws:states:::lambda:invoke':
      case 'arn:aws:states:::lambda:invoke.waitForTaskToken':
        return getLambdaPermissions.bind(this)(state);

      case 'arn:aws:states:::states:startExecution':
      case 'arn:aws:states:::states:startExecution.sync':
      case 'arn:aws:states:::states:startExecution.sync:2':
      case 'arn:aws:states:::states:startExecution.waitForTaskToken':
        return getStepFunctionsPermissions(state);

      case 'arn:aws:states:::aws-sdk:sfn:startSyncExecution':
        return getStepFunctionsSDKPermissions(state);

      case 'arn:aws:states:::codebuild:startBuild':
      case 'arn:aws:states:::codebuild:startBuild.sync':
        return getCodeBuildPermissions(state);

      case 'arn:aws:states:::sagemaker:createTransformJob.sync':
        return getSageMakerPermissions(state);

      case 'arn:aws:states:::bedrock:invokeModel':
        return getBedrockPermissions(state);

      case 'arn:aws:states:::events:putEvents':
      case 'arn:aws:states:::events:putEvents.waitForTaskToken':
        return getEventBridgePermissions(state);

      case 'arn:aws:states:::aws-sdk:scheduler:createSchedule':
        return getEventBridgeSchedulerPermissions(state);

      case 'arn:aws:states:::s3:getObject':
      case 'arn:aws:states:::aws-sdk:s3:getObject':
        return getS3ObjectPermissions('s3:GetObject', state);
      case 'arn:aws:states:::s3:putObject':
      case 'arn:aws:states:::aws-sdk:s3:putObject':
        return getS3ObjectPermissions('s3:PutObject', state);
      case 'arn:aws:states:::s3:listObjectsV2':
      case 'arn:aws:states:::aws-sdk:s3:listObjectsV2':
        return getS3ObjectPermissions('s3:listObjectsV2', state);

      default:
        if (isIntrinsic(state.Resource) || !!state.Resource.match(/arn:aws(-[a-z]+)*:lambda/)) {
          const trimmedArn = trimAliasFromLambdaArn(state.Resource);
          const functionArn = translateLocalFunctionNames.bind(this)(trimmedArn);
          return [{
            action: 'lambda:InvokeFunction',
            resource: [
              functionArn,
              { 'Fn::Sub': ['${functionArn}:*', { functionArn }] },
            ],
          }];
        }
        logger.log('Cannot generate IAM policy statement for Task state', state);
        return [];
    }
  });
}

function getIamStatements(iamPermissions, stateMachineObj) {
  // when the state machine doesn't define any Task states, and therefore doesn't need ANY
  // permission, then we should follow the behaviour of the AWS console and return a policy
  // that denies access to EVERYTHING
  if (_.isEmpty(iamPermissions) && _.isEmpty(stateMachineObj.iamRoleStatements)) {
    return [{
      Effect: 'Deny',
      Action: '*',
      Resource: '*',
    }];
  }

  const iamStatements = iamPermissions.map(p => ({
    Effect: 'Allow',
    Action: p.action.split(','),
    Resource: p.resource,
  }));

  if (!_.isEmpty(stateMachineObj.iamRoleStatements)) {
    iamStatements.push(...stateMachineObj.iamRoleStatements);
  }

  return iamStatements;
}

module.exports = {
  compileIamRole() {
    logger.config(this.serverless, this.v3Api);
    const service = this.serverless.service;
    const permissionsBoundary = service.provider.rolePermissionsBoundary;
    this.getAllStateMachines().forEach((stateMachineId) => {
      const stateMachineObj = this.getStateMachine(stateMachineId);
      const stateMachineName = stateMachineObj.name || stateMachineId;
      if (stateMachineObj.role) {
        return;
      }

      if (!stateMachineObj.definition) {
        throw new Error(`Missing "definition" for state machine ${stateMachineId}`);
      }

      const taskStates = getTaskStates(stateMachineObj.definition.States, stateMachineName);
      let iamPermissions = getIamPermissions.bind(this)(taskStates);

      if (stateMachineObj.loggingConfig) {
        iamPermissions.push({
          action: 'logs:CreateLogDelivery,logs:GetLogDelivery,logs:UpdateLogDelivery,logs:DeleteLogDelivery,logs:ListLogDeliveries,logs:PutResourcePolicy,logs:DescribeResourcePolicies,logs:DescribeLogGroups',
          resource: '*',
        });
      }

      if (stateMachineObj.tracingConfig) {
        iamPermissions.push({
          action: 'xray:PutTraceSegments,xray:PutTelemetryRecords,xray:GetSamplingRules,xray:GetSamplingTargets',
          resource: '*',
        });
      }

      iamPermissions = consolidatePermissionsByAction(iamPermissions);
      iamPermissions = consolidatePermissionsByResource(iamPermissions);
      const iamStatements = getIamStatements(iamPermissions, stateMachineObj);

      const iamRoleStateMachineExecutionTemplate = this.serverless.utils.readFileSync(
        path.join(__dirname,
          '..',
          '..',
          'iam-role-statemachine-execution-template.txt'),
      );

      let iamRoleJson = iamRoleStateMachineExecutionTemplate
        .replace('[PolicyName]', this.getStateMachinePolicyName())
        .replace('[Statements]', JSON.stringify(iamStatements));

      if (permissionsBoundary) {
        const jsonIamRole = JSON.parse(iamRoleJson);
        jsonIamRole.Properties.PermissionsBoundary = permissionsBoundary;
        iamRoleJson = JSON.stringify(jsonIamRole);
      }

      const stateMachineLogicalId = this.getStateMachineLogicalId(
        stateMachineId,
        stateMachineObj,
      );
      const iamRoleStateMachineLogicalId = `${stateMachineLogicalId}Role`;
      const newIamRoleStateMachineExecutionObject = {
        [iamRoleStateMachineLogicalId]: JSON.parse(iamRoleJson),
      };

      _.merge(
        this.serverless.service.provider.compiledCloudFormationTemplate.Resources,
        newIamRoleStateMachineExecutionObject,
      );
    });

    return BbPromise.resolve();
  },
};
