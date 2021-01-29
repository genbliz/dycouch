import { LoggingService } from "../helpers/logging-service";
import AWS, { DynamoDB } from "aws-sdk";
import { DocumentClient } from "aws-sdk/clients/dynamodb";

class DynamoConnectionBase {
  private _dynamoDbClient: DocumentClient;
  private _dynamoDb: DynamoDB;

  constructor() {
    const region = "us-west-2";
    AWS.config.update({
      region,
    });
    const options: DynamoDB.ClientConfiguration = {
      apiVersion: "2012-08-10",
      region,
    };
    this._dynamoDb = new DynamoDB(options);
    this._dynamoDbClient = new DocumentClient(options);
    LoggingService.log(`Initialized DynamoDb, region: ${region}`);
  }

  dynamoDbInst() {
    return this._dynamoDb;
  }

  dynamoDbClientInst() {
    return this._dynamoDbClient;
  }
}

export const MyDynamoConnection = new DynamoConnectionBase();
