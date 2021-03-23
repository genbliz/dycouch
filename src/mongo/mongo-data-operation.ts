// import { LoggingService } from "./../helpers/logging-service";
import type {
  IFuseFieldCondition,
  IFuseIndexDefinition,
  IFusePagingResult,
  IFuseQueryIndexOptions,
} from "../type/types";
import { RepoModel } from "../model/repo-model";
import Joi from "joi";
import { coreSchemaDefinition, IFuseCoreEntityModel } from "../core/base-schema";
import { FuseErrorUtils, FuseGenericError } from "../helpers/errors";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { FuseInitializerMongo } from "./mongo-initializer";
import { MongoFilterQueryOperation } from "./mongo-filter-query-operation";
import { MongoManageTable } from "./mongo-table-manager";

interface IOptions<T> {
  schemaDef: Joi.SchemaMap;
  mongoDb: () => FuseInitializerMongo;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IFuseCoreEntityModel;

type IFullEntity<T> = IFuseCoreEntityModel & T;

export class MongoDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _fuse_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _fuse_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _fuse_operationNotSuccessful = "Operation Not Successful";
  private readonly _fuse_entityResultFieldKeysMap: Map<string, string>;
  private readonly _fuse_mongoDb: () => FuseInitializerMongo;
  private readonly _fuse_dataKeyGenerator: () => string;
  private readonly _fuse_schema: Joi.Schema;
  private readonly _fuse_tableFullName: string;
  private readonly _fuse_strictRequiredFields: string[];
  private readonly _fuse_featureEntityValue: string;
  private readonly _fuse_secondaryIndexOptions: IFuseIndexDefinition<T>[];
  private readonly _fuse_errorHelper: FuseErrorUtils;
  private readonly _fuse_filterQueryOperation = new MongoFilterQueryOperation();
  //
  private _fuse_tableManager!: MongoManageTable<T>;

  constructor({
    schemaDef,
    mongoDb,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IOptions<T>) {
    super();
    this._fuse_mongoDb = mongoDb;
    this._fuse_dataKeyGenerator = dataKeyGenerator;
    this._fuse_tableFullName = baseTableName;
    this._fuse_featureEntityValue = featureEntityValue;
    this._fuse_secondaryIndexOptions = secondaryIndexOptions;
    this._fuse_strictRequiredFields = strictRequiredFields as string[];
    this._fuse_errorHelper = new FuseErrorUtils();
    this._fuse_entityResultFieldKeysMap = new Map();

    const fullSchemaMapDef = { ...schemaDef, ...coreSchemaDefinition };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._fuse_entityResultFieldKeysMap.set(key, key);
    });

    this._fuse_schema = Joi.object().keys({
      ...fullSchemaMapDef,
      _id: Joi.string().required().min(5).max(1500),
    });
  }

  fuse_tableManager() {
    if (!this._fuse_tableManager) {
      this._fuse_tableManager = new MongoManageTable<T>({
        mongoDb: () => this._fuse_mongoDb(),
        secondaryIndexOptions: this._fuse_secondaryIndexOptions,
        tableFullName: this._fuse_tableFullName,
        partitionKeyFieldName: this._fuse_partitionKeyFieldName,
        sortKeyFieldName: this._fuse_sortKeyFieldName,
      });
    }
    return this._fuse_tableManager;
  }

  private _fuse_generateDynamoTableKey() {
    return this._fuse_dataKeyGenerator();
  }

  private async _fuse_getDbInstance() {
    return await this._fuse_mongoDb().getDbInstance<IFullEntity<T>>();
  }

  private _fuse_getLocalVariables() {
    return {
      partitionKeyFieldName: this._fuse_partitionKeyFieldName,
      sortKeyFieldName: this._fuse_sortKeyFieldName,
      //
      featureEntityValue: this._fuse_featureEntityValue,
      //
      // tableFullName: this._fuse_tableFullName,
      secondaryIndexOptions: this._fuse_secondaryIndexOptions,
      strictRequiredFields: this._fuse_strictRequiredFields,
    } as const;
  }

  private _fuse_getNativePouchId(dataId: string) {
    const { featureEntityValue } = this._fuse_getLocalVariables();
    return [featureEntityValue, dataId].join(":");
  }

  private _fuse_getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._fuse_getLocalVariables();

    const dataMust = {
      _id: this._fuse_getNativePouchId(dataId),
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _fuse_withConditionPassed({
    item,
    withCondition,
  }: {
    item: Record<keyof T, any>;
    withCondition?: IFuseFieldCondition<T>;
  }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  private _fuse_checkValidateMustBeAnObjectDataType(data: unknown) {
    if (!data || typeof data !== "object") {
      throw this._fuse_createGenericError(`Data MUST be valid object`);
    }
  }

  private _fuse_checkValidateStrictRequiredFields(onDataObj: any) {
    this._fuse_checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._fuse_getLocalVariables();

    if (strictRequiredFields?.length) {
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._fuse_createGenericError(`Strict required field NOT defined`);
        }
      }
    }
  }

  private _fuse_removeDuplicateString(list: string[]) {
    return Array.from(new Set(list));
  }

  private _fuse_toMongoProjection(fields?: (keyof T)[]) {
    if (fields?.length) {
      const projection: Record<string, any> = {};
      const uniqueFields = this._fuse_removeDuplicateString(fields as string[]);
      uniqueFields.forEach((field) => {
        projection[field] = 1;
      });
      return projection;
    }
    return undefined;
  }

  private async _fuse_allHelpValidateGetValue(data: any) {
    const { error, value } = this._fuse_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this._fuse_errorHelper.fuse_helper_createFriendlyError(msg);
    }

    return await Promise.resolve({ validatedData: value });
  }

  private _fuse_createGenericError(error: string) {
    return new FuseGenericError(error);
  }

  async fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T | null> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const db = await this._fuse_getDbInstance();

    const nativeId = this._fuse_getNativePouchId(dataId);
    const query: any = { _id: nativeId };
    const dataInDb = await db.findOne(query, { projection: { _id: 0 } });

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      return null;
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return dataInDb;
  }

  async fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T[]> {
    const uniqueIds = this._fuse_removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._fuse_getNativePouchId(id));

    const db = await this._fuse_getDbInstance();

    const projection = this._fuse_toMongoProjection(fields) ?? { _id: -1 };

    const query: any = { _id: { $in: fullUniqueIds } };

    return await db.find(query, { projection: projection }).toArray();
  }

  async fuse_createOne({ data }: { data: T }): Promise<T> {
    this._fuse_checkValidateStrictRequiredFields(data);

    const { partitionKeyFieldName, featureEntityValue } = this._fuse_getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._fuse_generateDynamoTableKey();
    }

    if (!(dataId && typeof dataId === "string")) {
      throw this._fuse_createGenericError("Invalid dataId generation");
    }

    const dataMust = this._fuse_getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    if (fullData.featureEntity !== featureEntityValue) {
      throw this._fuse_createGenericError("FeatureEntity mismatched");
    }

    const validated = await this._fuse_allHelpValidateGetValue(fullData);

    const db = await this._fuse_getDbInstance();

    const result = await db.insertOne(validated.validatedData);

    if (!result?.insertedCount) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    const final = { ...validated.validatedData };
    delete final._id;
    return final;
  }

  async fuse_updateOne({
    dataId,
    updateData,
    withCondition,
  }: {
    dataId: string;
    updateData: T;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const nativeId = this._fuse_getNativePouchId(dataId);
    const query: any = { _id: nativeId };

    const db = await this._fuse_getDbInstance();

    const dataInDb = await db.findOne(query);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      throw this._fuse_createGenericError("Record does not exists");
    }

    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists");
    }
    const data: IFullEntity<T> = { ...dataInDb, ...updateData } as any;

    const validated = await this._fuse_allHelpValidateGetValue(data);

    const result = await db.replaceOne(query, validated.validatedData);
    if (!result.modifiedCount) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    const final = { ...validated.validatedData };
    delete final._id;
    return final;
  }

  async fuse_getManyBySecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<T[]> {
    paramOption.pagingParams = undefined;
    const result = await this.fuse_getManyBySecondaryIndexPaginate(paramOption);
    if (result?.mainResult) {
      return result.mainResult;
    }
    return [];
  }

  async fuse_getManyBySecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQueryIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>> {
    const { secondaryIndexOptions } = this._fuse_getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw this._fuse_createGenericError("Invalid secondary index definitions");
    }

    if (!paramOption?.indexName) {
      throw this._fuse_createGenericError("Invalid index name input");
    }

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === paramOption.indexName;
    });

    if (!secondaryIndex) {
      throw this._fuse_createGenericError("Secondary index not named/defined");
    }

    const index_PartitionKeyFieldName = secondaryIndex.partitionKeyFieldName as string;
    const index_SortKeyFieldName = secondaryIndex.sortKeyFieldName as string;

    const partitionSortKeyQuery = paramOption.sortKeyQuery
      ? {
          ...{ [index_SortKeyFieldName]: paramOption.sortKeyQuery },
          ...{ [index_PartitionKeyFieldName]: paramOption.partitionKeyQuery.equals },
        }
      : { [index_PartitionKeyFieldName]: paramOption.partitionKeyQuery.equals };

    const localVariables = this._fuse_getLocalVariables();
    /** Avoid query data leak */
    const hasFeatureEntity = [
      //
      index_PartitionKeyFieldName,
      index_SortKeyFieldName,
    ].includes(localVariables.sortKeyFieldName);
    if (!hasFeatureEntity) {
      paramOption.query = {
        ...paramOption.query,
        ...{ [localVariables.sortKeyFieldName]: localVariables.featureEntityValue },
      } as any;
    } else if (index_PartitionKeyFieldName !== localVariables.sortKeyFieldName) {
      if (localVariables.sortKeyFieldName === index_SortKeyFieldName) {
        partitionSortKeyQuery[index_SortKeyFieldName] = { $eq: localVariables.featureEntityValue as any };
      }
    }

    const queryDefs = {
      ...paramOption.query,
      ...partitionSortKeyQuery,
    };

    const queryDefData: any = this._fuse_filterQueryOperation.processQueryFilter({ queryDefs });

    const db = await this._fuse_getDbInstance();

    const projection = this._fuse_toMongoProjection(paramOption.fields as any[]) ?? { _id: 0 };

    const sort01: Array<[string, number]> = [];

    if (paramOption?.pagingParams?.orderDesc) {
      sort01.push([index_PartitionKeyFieldName, -1]);
      sort01.push([index_SortKeyFieldName, -1]);
    } else {
      sort01.push([index_PartitionKeyFieldName, 1]);
      sort01.push([index_SortKeyFieldName, 1]);
    }

    const results = await db
      .find(queryDefData, {
        projection,
        sort: sort01.length ? sort01 : undefined,
        limit: paramOption?.pagingParams?.pageSize ?? undefined,
      })
      .toArray();

    return {
      mainResult: results,
    };
  }

  async fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const db = await this._fuse_getDbInstance();

    const nativeId = this._fuse_getNativePouchId(dataId);
    const query: any = { _id: nativeId };
    const dataInDb = await db.findOne(query);

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      throw this._fuse_createGenericError("Record does not exists");
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await db.deleteOne(query);
    if (!result?.deletedCount) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return dataInDb;
  }
}
