import { LoggingService } from "./../helpers/logging-service";
import type {
  IFuseFieldCondition,
  IFuseIndexDefinition,
  IFusePagingResult,
  IFuseQueryIndexOptions,
} from "../type/types";
import { RepoModel } from "../model/repo-model";
import Joi from "joi";
import type { FuseInitializerCouch } from "./couch-initializer";
import { coreSchemaDefinition, IFuseCoreEntityModel } from "../core/base-schema";
import { FuseErrorUtils, FuseGenericError } from "../helpers/errors";
import { getJoiValidationErrors } from "../helpers/base-joi-helper";
import { CouchFilterQueryOperation } from "./couch-filter-query-operation";
import { CouchManageTable } from "./couch-manage-table";

interface IDynamoOptions<T> {
  schemaDef: Joi.SchemaMap;
  couchDb: () => FuseInitializerCouch;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IFuseCoreEntityModel;

type IFullEntity<T> = IFuseCoreEntityModel & T;

export class CouchDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly _fuse_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly _fuse_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _fuse_operationNotSuccessful = "Operation Not Successful";
  private readonly _fuse_entityResultFieldKeysMap: Map<string, string>;
  private readonly _fuse_couchDb: () => FuseInitializerCouch;
  private readonly _fuse_dataKeyGenerator: () => string;
  private readonly _fuse_schema: Joi.Schema;
  private readonly _fuse_tableFullName: string;
  private readonly _fuse_strictRequiredFields: string[];
  private readonly _fuse_featureEntityValue: string;
  private readonly _fuse_secondaryIndexOptions: IFuseIndexDefinition<T>[];
  private readonly _fuse_errorHelper: FuseErrorUtils;
  private readonly _fuse_filterQueryOperation = new CouchFilterQueryOperation();
  //
  private _fuse_tableManager!: CouchManageTable<T>;

  constructor({
    schemaDef,
    couchDb,
    secondaryIndexOptions,
    featureEntityValue,
    baseTableName,
    strictRequiredFields,
    dataKeyGenerator,
  }: IDynamoOptions<T>) {
    super();
    this._fuse_couchDb = couchDb;
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
      this._fuse_tableManager = new CouchManageTable<T>({
        couchDb: () => this._fuse_couchDb(),
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

  private _fuse_couchDbInstance() {
    return this._fuse_couchDb().getDocInstance();
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

  private _fuse_stripNonRequiredOutputData({ dataObj, excludeFields }: { dataObj: any; excludeFields?: string[] }): T {
    const returnData = {} as any;
    if (typeof dataObj === "object" && this._fuse_entityResultFieldKeysMap.size > 0) {
      Object.entries(dataObj).forEach(([key, value]) => {
        if (this._fuse_entityResultFieldKeysMap.has(key)) {
          if (excludeFields?.length) {
            if (!excludeFields.includes(key)) {
              returnData[key] = value;
            }
          } else {
            returnData[key] = value;
          }
        }
      });
    }
    return returnData;
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

  private _fuse_withConditionPassed({ item, withCondition }: { item: any; withCondition?: IFuseFieldCondition<T> }) {
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

  async fuse_createOne({ data }: { data: T }): Promise<T> {
    this._fuse_checkValidateStrictRequiredFields(data);

    const { partitionKeyFieldName } = this._fuse_getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._fuse_generateDynamoTableKey();
    }

    if (!(dataId && typeof dataId === "string")) {
      throw this._fuse_createGenericError("Invalid dataId generation");
    }

    const dataMust = this._fuse_getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    const validated = await this._fuse_allHelpValidateGetValue(fullData);

    const result = await this._fuse_couchDbInstance().insert(validated.validatedData);
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({
      dataObj: validated.validatedData,
    });
  }

  async fuse_getAll({ size, skip }: { size?: number; skip?: number } = {}): Promise<T[]> {
    const data = await this._fuse_couchDbInstance().list({
      include_docs: true,
      startkey: this._fuse_featureEntityValue,
      endkey: `${this._fuse_featureEntityValue}\ufff0`,
      limit: size,
      skip,
    });
    const dataList: T[] = [];
    data?.rows?.forEach((item) => {
      if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
        const doc = this._fuse_stripNonRequiredOutputData({ dataObj: item.doc });
        dataList.push(doc);
      }
    });
    return dataList;
  }

  async fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T | null> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const nativeId = this._fuse_getNativePouchId(dataId);

    const dataInDb = await this._fuse_couchDbInstance().get(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      return null;
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: dataInDb });
  }

  async fuse_updateOneById({
    dataId,
    data,
    withCondition,
  }: {
    dataId: string;
    data: T;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId });

    const nativeId = this._fuse_getNativePouchId(dataId);

    const dataInDb = await this._fuse_couchDbInstance().get(nativeId);
    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue && dataInDb._rev)) {
      throw this._fuse_createGenericError("Record does not exists");
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists");
    }
    const _data: IFullEntity<T> = { ...data } as any;

    const validated = await this._fuse_allHelpValidateGetValue(_data);

    const result = await this._fuse_couchDbInstance().insert({
      ...validated.validatedData,
      _rev: dataInDb._rev,
    });
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({
      dataObj: validated.validatedData,
    });
  }

  fuse_updateOneDirect({ data }: { data: T }): Promise<T> {
    const dataInput: IFullEntity<T> = data as any;
    this._fuse_errorHelper.fuse_helper_validateRequiredString({ dataId: dataInput.id });
    return this.fuse_updateOneById({ data, dataId: dataInput.id });
  }

  async fuse_getManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[];
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T[]> {
    //
    const uniqueIds = this._fuse_removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._fuse_getNativePouchId(id));

    const data = await this._fuse_couchDbInstance().list({
      keys: fullUniqueIds,
      include_docs: true,
    });

    const dataList: T[] = [];

    if (withCondition?.length) {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
          const passed = this._fuse_withConditionPassed({ item: item.doc, withCondition });
          if (passed) {
            const k = this._fuse_stripNonRequiredOutputData({
              dataObj: item.doc,
              excludeFields: fields as any[],
            });
            dataList.push(k);
          }
        }
      });
    } else {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this._fuse_featureEntityValue) {
          const k = this._fuse_stripNonRequiredOutputData({
            dataObj: item.doc,
            excludeFields: fields as any[],
          });
          dataList.push(k);
        }
      });
    }
    return dataList;
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

    const partitionKeyFieldName = secondaryIndex.keyFieldName as string;
    const sortKeyFieldName = secondaryIndex.sortFieldName as string;

    const partitionSortKeyQuery = paramOption.sortKeyQuery
      ? {
          ...{ [sortKeyFieldName]: paramOption.sortKeyQuery },
          ...{ [partitionKeyFieldName]: paramOption.partitionKeyQuery.equals },
        }
      : { [partitionKeyFieldName]: paramOption.partitionKeyQuery.equals };

    const queryDefs = {
      ...paramOption.query,
      ...partitionSortKeyQuery,
    };

    const queryDefData = this._fuse_filterQueryOperation.processQueryFilter({
      queryDefs,
    });

    const query01: any = {};
    const query02: any = {};
    const query03: any = {};

    Object.entries(queryDefData).forEach(([key, val]) => {
      if (key === partitionKeyFieldName) {
        query01[key] = val;
      } else if (key === sortKeyFieldName) {
        query02[key] = val;
      } else {
        query03[key] = val;
      }
    });

    const queryDefDataOrdered = { ...query01, ...query02, ...query03 };
    const sort01: Array<{ [propName: string]: "asc" | "desc" }> = [];

    if (paramOption?.pagingParams?.orderDesc) {
      sort01.push({ [partitionKeyFieldName]: "desc" });
      sort01.push({ [sortKeyFieldName]: "desc" });
    } else {
      sort01.push({ [partitionKeyFieldName]: "asc" });
      sort01.push({ [sortKeyFieldName]: "asc" });
    }

    LoggingService.log({
      queryDefDataOrdered,
      sort: sort01,
      paramOption,
    });

    const data = await this._fuse_couchDbInstance().partitionedFind(this._fuse_featureEntityValue, {
      selector: { ...queryDefDataOrdered },
      fields: paramOption?.fields?.length ? this._fuse_removeDuplicateString(paramOption.fields as any) : undefined,
      use_index: paramOption.indexName,
      sort: sort01?.length ? sort01 : undefined,
      limit: paramOption?.pagingParams?.pageSize ?? undefined,
    });

    const dataList = data?.docs?.map((item) => {
      return this._fuse_stripNonRequiredOutputData({ dataObj: item });
    });

    return {
      mainResult: dataList,
    };
  }

  async fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    const nativeId = this._fuse_getNativePouchId(dataId);
    const dataInDb = await this._fuse_couchDbInstance().get(nativeId);

    if (!(dataInDb?.id === dataId && dataInDb.featureEntity === this._fuse_featureEntityValue)) {
      throw this._fuse_createGenericError("Record does not exists");
    }
    const passed = this._fuse_withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._fuse_createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await this._fuse_couchDbInstance().destroy(dataInDb._id, dataInDb._rev);
    if (!result.ok) {
      throw this._fuse_createGenericError(this._fuse_operationNotSuccessful);
    }
    return this._fuse_stripNonRequiredOutputData({ dataObj: dataInDb });
  }
}
