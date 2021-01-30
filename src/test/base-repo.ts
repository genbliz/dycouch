import { ISecondaryIndexDef } from "../types";
import { FuseDataOperationDynamo, FuseInitializerDynamo } from "../";
import Joi from "joi";

interface IBaseRepoOptions<T> {
  schemaSubDef: Joi.SchemaMap;
  featureEntityValue: string;
  secondaryIndexOptions: ISecondaryIndexDef<T>[];
}

export abstract class BaseRepository<T> extends FuseDataOperationDynamo<T> {
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
