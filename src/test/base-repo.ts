import { ISecondaryIndexDef } from "../types";
import DynamoDataOpr from "../core/dynamo-data-operation";
import { MyDynamoConnection } from "../test/connection";
import Joi from "joi";

interface IBaseRepoOptions<T> {
  schemaSubDef: Joi.SchemaMap;
  featureEntityValue: string;
  secondaryIndexOptions: ISecondaryIndexDef<T>[];
}

export abstract class BaseRepository<T> extends DynamoDataOpr<T> {
  constructor({
    schemaSubDef,
    secondaryIndexOptions,
    featureEntityValue,
  }: IBaseRepoOptions<T>) {
    super({
      dynamoDb: () => MyDynamoConnection.dynamoDbInst(),
      dynamoDbClient: () => MyDynamoConnection.dynamoDbClientInst(),
      baseTableName: "hospiman_table_db1",
      schemaDef: { ...schemaSubDef },
      secondaryIndexOptions,
      featureEntityValue: featureEntityValue,
      strictRequiredFields: [],
      dataKeyGenerator: () => Date.now().toString(),
    });
  }
}
