import { LoggingService } from "../helpers/logging-service";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

class DynamoConnectionBase {
  private _dynamoDbClient: DynamoDBClient;

  constructor() {
    const region = "us-west-2";
    this._dynamoDbClient = new DynamoDBClient({
      apiVersion: "2012-08-10",
      region,
    });
    LoggingService.log(`Initialized DynamoDb, region: ${region}`);
  }

  dynamoDbClientInst() {
    return this._dynamoDbClient;
  }
}

export const MyDynamoConnection = new DynamoConnectionBase();
