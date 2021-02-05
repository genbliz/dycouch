import { FuseInitializerDynamo } from "./../dynamo/dynamo-initializer";
import { DynamoDataOperation } from "./../dynamo/dynamo-data-operation";
import type { IFuseIndexDefinition } from "../type/types";
import Joi from "joi";

interface IBaseRepoOptions<T> {
  schemaSubDef: Joi.SchemaMap;
  featureEntityValue: string;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
}

export abstract class BaseRepository<T> extends DynamoDataOperation<T> {
  constructor({ schemaSubDef, secondaryIndexOptions, featureEntityValue }: IBaseRepoOptions<T>) {
    super({
      dynamoDb: () => new FuseInitializerDynamo({ region: "" }),
      baseTableName: "hospiman_table_db1",
      schemaDef: { ...schemaSubDef },
      secondaryIndexOptions,
      featureEntityValue: featureEntityValue,
      strictRequiredFields: [],
      dataKeyGenerator: () => Date.now().toString(),
    });
  }
}
