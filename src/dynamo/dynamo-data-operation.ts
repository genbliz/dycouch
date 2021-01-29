import type {
  IDynamoQueryParamOptions,
  ISecondaryIndexDef,
  IFieldCondition,
  IDynamoQuerySecondayIndexOptions,
} from "../types";
import { GenericDataError } from "./../helpers/errors";
import type { DynamoDB } from "aws-sdk";
import type { DocumentClient } from "aws-sdk/clients/dynamodb";
import Joi from "joi";
import { Marshaller } from "@aws/dynamodb-auto-marshaller";
import { getJoiValidationErrors } from "../core/base-joi-helper";
import BaseMixins from "../core/base-mixins";
import { coreSchemaDefinition, IDynamoDataCoreEntityModel } from "./base-schema";
import { DynamoManageTable } from "./dynamo-manage-table";
import { LoggingService } from "../helpers/logging-service";

interface IDynamoOptions<T> {
  dynamoDb: () => DynamoDB;
  schemaDef: Joi.SchemaMap;
  dynamoDbClient: () => DocumentClient;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: ISecondaryIndexDef<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

function createTenantSchema(schemaMapDef: Joi.SchemaMap) {
  return Joi.object().keys({ ...schemaMapDef, ...coreSchemaDefinition });
}

type IModelKeys = keyof IDynamoDataCoreEntityModel;

export default abstract class DynamoDataOperation<T> extends BaseMixins {
  private readonly here_partitionKeyFieldName: IModelKeys = "id";
  private readonly here_sortKeyFieldName: IModelKeys = "featureEntity";
  //
  private readonly here_dynamoDbClient: () => DocumentClient;
  private readonly here_dynamoDb: () => DynamoDB;
  private readonly here_dataKeyGenerator: () => string;
  private readonly here_schema: Joi.Schema;
  private readonly here_marshaller: Marshaller;
  private readonly here_tableFullName: string;
  private readonly here_strictRequiredFields: string[];
  private readonly here_featureEntityValue: string;
  private readonly here_secondaryIndexOptions: ISecondaryIndexDef<T>[];
  //
  private here_tableManager!: DynamoManageTable<T>;

  constructor({
    dynamoDb,
    schemaDef,
    dynamoDbClient,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IDynamoOptions<T>) {
    super();
    this.here_dynamoDb = dynamoDb;
    this.here_dynamoDbClient = dynamoDbClient;
    this.here_dataKeyGenerator = dataKeyGenerator;
    this.here_schema = createTenantSchema(schemaDef);
    this.here_tableFullName = baseTableName;
    this.here_marshaller = new Marshaller({ onEmpty: "omit" });
    this.here_featureEntityValue = featureEntityValue;
    this.here_secondaryIndexOptions = secondaryIndexOptions;
    this.here_strictRequiredFields = strictRequiredFields as string[];
  }

  protected ddo_tableManager() {
    if (!this.here_tableManager) {
      this.here_tableManager = new DynamoManageTable<T>({
        dynamoDb: this.here_dynamoDb,
        secondaryIndexOptions: this.here_secondaryIndexOptions,
        tableFullName: this.here_tableFullName,
        partitionKeyFieldName: this.here_partitionKeyFieldName,
        sortKeyFieldName: this.here_sortKeyFieldName,
      });
    }
    return this.here_tableManager;
  }

  private _dynamoDbClient(): DocumentClient {
    return this.here_dynamoDbClient();
  }

  private _dynamoDb(): DynamoDB {
    return this.here_dynamoDb();
  }

  private _generateDynamoTableKey() {
    return this.here_dataKeyGenerator();
  }

  private _getLocalVariables() {
    return {
      partitionKeyFieldName: this.here_partitionKeyFieldName,
      sortKeyFieldName: this.here_sortKeyFieldName,
      //
      featureEntityValue: this.here_featureEntityValue,
      //
      tableFullName: this.here_tableFullName,
      secondaryIndexOptions: this.here_secondaryIndexOptions,
      strictRequiredFields: this.here_strictRequiredFields,
    } as const;
  }

  private _getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._getLocalVariables();

    const dataMust = {
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _checkValidateMustBeAnObjectDataType(data: any) {
    if (!data || typeof data !== "object") {
      throw new GenericDataError(`Data MUST be valid object`);
    }
  }

  private _checkValidateStrictRequiredFields(onDataObj: any) {
    this._checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._getLocalVariables();

    if (strictRequiredFields?.length) {
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw new GenericDataError(`Strict required field NOT defined`);
        }
      }
    }
  }

  protected async ddo_createOne({ data }: { data: T }) {
    this._checkValidateStrictRequiredFields(data);

    const { tableFullName, partitionKeyFieldName } = this._getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._generateDynamoTableKey();
    }

    const dataMust = this._getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    const { validatedData, marshalled } = await this._allHelpValidateMarshallAndGetValue(fullData);

    const params: DocumentClient.PutItemInput = {
      TableName: tableFullName,
      Item: marshalled,
    };

    await this._dynamoDb().putItem(params).promise();
    const result: T = validatedData;
    return result;
  }

  private _withConditionPassed({ item, withCondition }: { item: any; withCondition?: IFieldCondition<T> }) {
    if (item && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  protected async ddo_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFieldCondition<T>;
  }): Promise<T | null> {
    const {
      //
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
      tableFullName,
    } = this._getLocalVariables();

    this.allHelpValidateRequiredString({
      QueryGetOnePartitionKey: dataId,
      QueryGetOneSortKey: featureEntityValue,
    });

    const params: DocumentClient.GetItemInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: dataId,
        [sortKeyFieldName]: featureEntityValue,
      },
    };
    const result = await this._dynamoDbClient().get(params).promise();
    const item = result.Item as any;
    if (!item) {
      return null;
    }
    const isPassed = this._withConditionPassed({ withCondition, item });
    if (!isPassed) {
      return null;
    }
    return item;
  }

  protected async ddo_updateOneDirect({ data }: { data: T }) {
    this._checkValidateStrictRequiredFields(data);

    const { tableFullName, partitionKeyFieldName } = this._getLocalVariables();

    const dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      throw new GenericDataError("Update data requires sort key field value");
    }

    const dataMust = this._getBaseObject({ dataId });

    const fullData = { ...data, ...dataMust };
    //
    const { validatedData, marshalled } = await this._allHelpValidateMarshallAndGetValue(fullData);

    const params: DocumentClient.PutItemInput = {
      TableName: tableFullName,
      Item: marshalled,
    };

    await this._dynamoDb().putItem(params).promise();
    const result: T = validatedData;
    return result;
  }

  protected async ddo_updateOneById({
    dataId,
    data,
    withCondition,
  }: {
    dataId: string;
    data: T;
    withCondition?: IFieldCondition<T>;
  }) {
    this._checkValidateStrictRequiredFields(data);

    const { tableFullName, partitionKeyFieldName } = this._getLocalVariables();

    this.allHelpValidateRequiredString({ Update1DataId: dataId });

    const dataInDb = await this.ddo_getOneById({ dataId });

    if (!(dataInDb && dataInDb[partitionKeyFieldName])) {
      throw this.allHelpCreateFriendlyError("Data does NOT exists");
    }

    const isPassed = this._withConditionPassed({
      withCondition,
      item: dataInDb,
    });
    if (!isPassed) {
      throw new GenericDataError("Update condition failed");
    }

    const dataMust = this._getBaseObject({
      dataId: dataInDb[partitionKeyFieldName],
    });

    const fullData = { ...dataInDb, ...data, ...dataMust };

    const { validatedData, marshalled } = await this._allHelpValidateMarshallAndGetValue(fullData);

    const params: DocumentClient.PutItemInput = {
      TableName: tableFullName,
      Item: marshalled,
    };

    await this._dynamoDb().putItem(params).promise();
    const result: T = validatedData;
    return result;
  }

  private async _allHelpValidateMarshallAndGetValue(data: any) {
    const { error, value } = this.here_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this.allHelpCreateFriendlyError(msg);
    }
    const marshalledData = this.here_marshaller.marshallItem(value);

    return await Promise.resolve({
      validatedData: value,
      marshalled: marshalledData,
    });
  }

  private _removeDuplicateString<T = string>(strArray: T[]) {
    return Array.from(new Set([...strArray]));
  }

  protected async ddo_getManyByCondition(paramOptions: IDynamoQueryParamOptions<T>) {
    paramOptions.pagingParams = undefined;
    const result = await this.ddo_getManyByConditionPaginate(paramOptions);
    if (result?.mainResult?.length) {
      return result.mainResult;
    }
    return [];
  }

  protected async ddo_getManyByConditionPaginate(paramOptions: IDynamoQueryParamOptions<T>) {
    const { tableFullName, sortKeyFieldName, partitionKeyFieldName } = this._getLocalVariables();
    //
    if (!paramOptions?.partitionKeyQuery?.equals === undefined) {
      throw new GenericDataError("Invalid Hash key value");
    }
    if (!sortKeyFieldName) {
      throw new GenericDataError("Bad query sort configuration");
    }

    let sortKeyQuery: any = {};

    const sortKeyQueryData = paramOptions.sortKeyQuery;
    if (sortKeyQueryData) {
      if (sortKeyQueryData[sortKeyFieldName]) {
        sortKeyQuery = {
          [sortKeyFieldName]: sortKeyQueryData[sortKeyFieldName],
        };
      } else {
        throw new GenericDataError("Invalid Sort key value");
      }
    }

    const fieldKeys = paramOptions?.fields?.length ? this._removeDuplicateString(paramOptions.fields) : undefined;

    const filterHashSortKey = this.ddo__helperDynamoFilterOperation({
      queryDefs: {
        ...sortKeyQuery,
        ...{
          [partitionKeyFieldName]: paramOptions.partitionKeyQuery.equals,
        },
      },
      projectionFields: fieldKeys,
    });
    //
    //
    let otherFilterExpression: string | undefined = undefined;
    let otherExpressionAttributeValues: any = undefined;
    let otherExpressionAttributeNames: any = undefined;
    if (paramOptions?.query) {
      const filterOtherAttr = this.ddo__helperDynamoFilterOperation({
        queryDefs: paramOptions.query,
        projectionFields: null,
      });

      otherExpressionAttributeValues = filterOtherAttr.expressionAttributeValues;
      otherExpressionAttributeNames = filterOtherAttr.expressionAttributeNames;

      if (filterOtherAttr?.filterExpression && filterOtherAttr?.filterExpression.length > 1) {
        otherFilterExpression = filterOtherAttr.filterExpression;
      }
    }

    const params: DocumentClient.QueryInput = {
      TableName: tableFullName,
      KeyConditionExpression: filterHashSortKey.filterExpression,
      ExpressionAttributeValues: {
        ...otherExpressionAttributeValues,
        ...filterHashSortKey.expressionAttributeValues,
      },
      FilterExpression: otherFilterExpression ?? undefined,
      ExpressionAttributeNames: {
        ...otherExpressionAttributeNames,
        ...filterHashSortKey.expressionAttributeNames,
      },
    };

    if (filterHashSortKey?.projectionExpressionAttr) {
      params.ProjectionExpression = filterHashSortKey.projectionExpressionAttr;
    }

    if (paramOptions?.pagingParams?.orderDesc === true) {
      params.ScanIndexForward = false;
    }

    const hashKeyAndSortKey: [string, string] = [partitionKeyFieldName, sortKeyFieldName];

    const paginationObjects = { ...paramOptions.pagingParams };
    const result = await this.ddo__helperDynamoQueryProcessor<T>({
      dynamoDbClient: () => this._dynamoDbClient(),
      params,
      hashKeyAndSortKey,
      ...paginationObjects,
    });
    return result;
  }

  protected async ddo_batchGetManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFieldCondition<T>;
  }) {
    dataIds.forEach((dataId) => {
      this.allHelpValidateRequiredString({
        BatchGetDataId: dataId,
      });
    });

    const originalIds = this._removeDuplicateString(dataIds);
    const BATCH_SIZE = 80;

    const batchIds: string[][] = [];

    while (originalIds.length > 0) {
      const ids = originalIds.splice(0, BATCH_SIZE);
      batchIds.push(ids);
    }

    LoggingService.log("@allBatchGetManyByIdsBase batchIds: ", batchIds.length);

    let result: T[] = [];

    const fieldKeys = fields?.length ? this._removeDuplicateString(fields) : fields;

    for (const batch of batchIds) {
      const call = await this.__allBatchGetManyByIdsBasePrivate({
        dataIds: batch,
        fields: fieldKeys,
        withCondition,
      });
      result = [...result, ...call];
    }
    LoggingService.log("@allBatchGetManyByIdsBase batchIds result Out", result.length);
    return result;
  }

  private async __allBatchGetManyByIdsBasePrivate({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFieldCondition<T>;
  }) {
    return new Promise<T[]>((resolve, reject) => {
      const getRandom = () =>
        [
          "rand",
          Math.round(Math.random() * 99999),
          Math.round(Math.random() * 88888),
          Math.round(Math.random() * 99),
        ].join("");

      const {
        //
        tableFullName,
        partitionKeyFieldName,
        sortKeyFieldName,
        featureEntityValue,
      } = this._getLocalVariables();

      const dataIdsNoDup = this._removeDuplicateString(dataIds);

      const getArray: DynamoDB.Key[] = dataIdsNoDup.map((dataId) => {
        const params01 = {
          [partitionKeyFieldName]: { S: dataId },
          [sortKeyFieldName]: { S: featureEntityValue },
        };
        return params01;
      });

      let projectionExpression: string | undefined = undefined;
      let expressionAttributeNames: DynamoDB.ExpressionAttributeNameMap | undefined = undefined;

      if (fields?.length) {
        const fieldKeys = this._removeDuplicateString(fields);
        if (withCondition?.length) {
          /** Add excluded condition */
          withCondition.forEach((condition) => {
            if (!fieldKeys.includes(condition.field)) {
              fieldKeys.push(condition.field);
            }
          });
        }
        expressionAttributeNames = {};
        fieldKeys.forEach((fieldName) => {
          if (typeof fieldName === "string") {
            if (expressionAttributeNames) {
              const attrKeyHash = `#attrKey${getRandom()}k`.toLowerCase();
              expressionAttributeNames[attrKeyHash] = fieldName;
            }
          }
        });
        if (Object.keys(expressionAttributeNames)?.length) {
          projectionExpression = Object.keys(expressionAttributeNames).join(",");
        } else {
          projectionExpression = undefined;
          expressionAttributeNames = undefined;
        }
      }

      const params: DynamoDB.BatchGetItemInput = {
        RequestItems: {
          [tableFullName]: {
            Keys: [...getArray],
            ConsistentRead: true,
            ProjectionExpression: projectionExpression,
            ExpressionAttributeNames: expressionAttributeNames,
          },
        },
      };

      let returnedItems: any[] = [];

      const resolveItemResults = (resultItems: any[]) => {
        if (resultItems?.length && withCondition?.length) {
          return resultItems.filter((item) => {
            return withCondition.every((condition) => {
              return item[condition.field] === condition.equals;
            });
          });
        }
        return resultItems || [];
      };

      const batchGetUntilDone = (err: AWS.AWSError, data: DynamoDB.BatchGetItemOutput) => {
        if (err) {
          if (returnedItems?.length) {
            resolve(resolveItemResults(returnedItems));
          } else {
            reject(err.stack);
          }
        } else {
          if (data?.Responses) {
            const itemListRaw = data.Responses[tableFullName];
            const itemList = itemListRaw.map((item) => {
              return this.here_marshaller.unmarshallItem(item);
            });
            returnedItems = [...returnedItems, ...itemList];
          }

          if (data?.UnprocessedKeys && Object.keys(data.UnprocessedKeys).length) {
            const _params: DynamoDB.BatchGetItemInput = {
              RequestItems: data.UnprocessedKeys,
            };
            LoggingService.log({ dynamoBatchGetParams: _params });
            this._dynamoDb().batchGetItem(_params, batchGetUntilDone);
          } else {
            resolve(resolveItemResults(returnedItems));
          }
        }
      };
      this._dynamoDb().batchGetItem(params, batchGetUntilDone);
    });
  }

  protected async ddo_getSecondaryIndex<TData = T, TSortKeyField = string>(
    paramOption: IDynamoQuerySecondayIndexOptions<TData, TSortKeyField>,
  ) {
    paramOption.pagingParams = undefined;
    const result = await this.ddo_getSecondaryIndexPaginate<TData, TSortKeyField>(paramOption);
    if (result?.mainResult) {
      return result.mainResult;
    }
    return [];
  }

  protected async ddo_getSecondaryIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IDynamoQuerySecondayIndexOptions<TData, TSortKeyField>,
  ) {
    const { tableFullName, secondaryIndexOptions } = this._getLocalVariables();

    if (!secondaryIndexOptions?.length) {
      throw new GenericDataError("Invalid secondary index definitions");
    }

    const {
      //
      indexName,
      partitionKeyQuery,
      sortKeyQuery,
      fields,
      pagingParams,
      query,
    } = paramOption;

    const secondaryIndex = secondaryIndexOptions.find((item) => {
      return item.indexName === indexName;
    });

    if (!secondaryIndex) {
      throw new GenericDataError("Secondary index not named/defined");
    }

    const partitionKeyFieldName = secondaryIndex.keyFieldName as string;
    const sortKeyFieldName = secondaryIndex.sortFieldName as string;

    const partitionSortKeyQuery = sortKeyQuery
      ? {
          ...{ [sortKeyFieldName]: sortKeyQuery },
          ...{ [partitionKeyFieldName]: partitionKeyQuery.equals },
        }
      : { [partitionKeyFieldName]: partitionKeyQuery.equals };

    const fieldKeys = fields?.length ? this._removeDuplicateString(fields) : undefined;

    const {
      expressionAttributeValues,
      filterExpression,
      projectionExpressionAttr,
      expressionAttributeNames,
    } = this.ddo__helperDynamoFilterOperation({
      queryDefs: partitionSortKeyQuery,
      projectionFields: fieldKeys,
    });

    let otherFilterExpression: string | undefined = undefined;
    let otherExpressionAttributeValues: any = undefined;
    let otherExpressionAttributeNames: any = undefined;
    if (query) {
      const otherAttr = this.ddo__helperDynamoFilterOperation({
        queryDefs: query,
        projectionFields: null,
      });

      otherExpressionAttributeValues = otherAttr.expressionAttributeValues;
      otherExpressionAttributeNames = otherAttr.expressionAttributeNames;

      if (otherAttr?.filterExpression?.length && otherAttr?.filterExpression.length > 1) {
        otherFilterExpression = otherAttr.filterExpression;
      }
    }

    const params: DocumentClient.QueryInput = {
      TableName: tableFullName,
      IndexName: indexName,
      KeyConditionExpression: filterExpression,
      ExpressionAttributeValues: {
        ...otherExpressionAttributeValues,
        ...expressionAttributeValues,
      },
      FilterExpression: otherFilterExpression ?? undefined,
      ExpressionAttributeNames: {
        ...otherExpressionAttributeNames,
        ...expressionAttributeNames,
      },
    };

    const orderDesc = pagingParams?.orderDesc === true;

    if (orderDesc) {
      params.ScanIndexForward = false;
    }

    if (projectionExpressionAttr) {
      params.ProjectionExpression = projectionExpressionAttr;
    }

    const hashKeyAndSortKey: [string, string] = [partitionKeyFieldName, sortKeyFieldName];

    const result = await this.ddo__helperDynamoQueryProcessor<T>({
      dynamoDbClient: () => this._dynamoDbClient(),
      params,
      orderDesc,
      hashKeyAndSortKey,
      ...pagingParams,
    });
    return result;
  }

  protected async ddo_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFieldCondition<T>;
  }): Promise<T> {
    //
    this.allHelpValidateRequiredString({ Del1SortKey: dataId });
    const { tableFullName, partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._getLocalVariables();

    const dataExist = await this.ddo_getOneById({ dataId, withCondition });

    if (!(dataExist && dataExist[partitionKeyFieldName])) {
      throw this.allHelpCreateFriendlyError("Record does NOT exists");
    }

    const params: DocumentClient.DeleteItemInput = {
      TableName: tableFullName,
      Key: {
        [partitionKeyFieldName]: dataId,
        [sortKeyFieldName]: featureEntityValue,
      },
    };

    await this._dynamoDbClient().delete(params).promise();
    return dataExist;
  }

  protected async ddo_deleteManyDangerouselyByIds({ dataIds }: { dataIds: string[] }): Promise<boolean> {
    //
    const dataIdsNoDuplicates = this._removeDuplicateString(dataIds);
    dataIdsNoDuplicates.forEach((sortKeyValue) => {
      this.allHelpValidateRequiredString({
        DelSortKey: sortKeyValue,
      });
    });

    const {
      //
      tableFullName,
      partitionKeyFieldName,
      sortKeyFieldName,
      featureEntityValue,
    } = this._getLocalVariables();

    const delArray = dataIdsNoDuplicates.map((dataId) => {
      const params01: DynamoDB.WriteRequest = {
        DeleteRequest: {
          Key: {
            [partitionKeyFieldName]: { S: dataId },
            [sortKeyFieldName]: { S: featureEntityValue },
          },
        },
      };
      return params01;
    });

    const params: DynamoDB.BatchWriteItemInput = {
      RequestItems: {
        [tableFullName]: delArray,
      },
    };

    await this._dynamoDb().batchWriteItem(params).promise();
    return true;
  }
}
