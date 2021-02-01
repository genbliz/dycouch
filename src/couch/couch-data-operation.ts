import { IFieldCondition, ISecondaryIndexDef } from "../types/index";
import { IFuseFieldCondition, IFuseQueryParamOptions, IFusePagingResult, IFuseQuerySecondayIndexOptions } from "src";
import { RepoModel } from "../model/repo-model";
import Joi from "joi";
import { CouchInitializer } from "./couch-initializer";
import { coreSchemaDefinition, IDynamoDataCoreEntityModel } from "../core/base-schema";
import { FuseErrorUtils, GenericDataError } from "src/helpers/errors";
import { getJoiValidationErrors } from "src/helpers/base-joi-helper";

interface IDynamoOptions<T> {
  schemaDef: Joi.SchemaMap;
  couchDb: () => CouchInitializer;
  dataKeyGenerator: () => string;
  featureEntityValue: string;
  secondaryIndexOptions: ISecondaryIndexDef<T>[];
  baseTableName: string;
  strictRequiredFields: (keyof T)[] | string[];
}

type IModelBase = IDynamoDataCoreEntityModel;

type IFullEntity<T> = IDynamoDataCoreEntityModel & T;

export class CouchDataOperation<T> extends RepoModel<T> implements RepoModel<T> {
  private readonly here_partitionKeyFieldName: keyof Pick<IModelBase, "id"> = "id";
  private readonly here_sortKeyFieldName: keyof Pick<IModelBase, "featureEntity"> = "featureEntity";
  //
  private readonly _operationNotSuccessful = "Operation Not Successful";
  private readonly _entityResultFieldKeysMap: Map<string, string>;
  private readonly here_couchDb: () => CouchInitializer;
  private readonly here_dataKeyGenerator: () => string;
  private readonly here_schema: Joi.Schema;
  private readonly here_tableFullName: string;
  private readonly here_strictRequiredFields: string[];
  private readonly here_featureEntityValue: string;
  private readonly here_secondaryIndexOptions: ISecondaryIndexDef<T>[];
  private readonly errorHelper: FuseErrorUtils;

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
    this.here_couchDb = couchDb;
    this.here_dataKeyGenerator = dataKeyGenerator;
    this.here_tableFullName = baseTableName;
    this.here_featureEntityValue = featureEntityValue;
    this.here_secondaryIndexOptions = secondaryIndexOptions;
    this.here_strictRequiredFields = strictRequiredFields as string[];
    this.errorHelper = new FuseErrorUtils();
    this._entityResultFieldKeysMap = new Map();

    const fullSchemaMapDef = { ...schemaDef, ...coreSchemaDefinition };

    Object.keys(fullSchemaMapDef).forEach((key) => {
      this._entityResultFieldKeysMap.set(key, key);
    });

    this.here_schema = Joi.object().keys({
      ...fullSchemaMapDef,
      _id: Joi.string().required().min(5).max(1500),
    });
  }

  private _generateDynamoTableKey() {
    return this.here_dataKeyGenerator();
  }

  private _couchDbInstance() {
    return this.here_couchDb().getInstance();
  }

  // export type IFullEntity<T> = ICoreEntityBaseModel & T;

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

  private _stripNonRequiredOutputData({ dataObj, excludeFields }: { dataObj: any; excludeFields?: string[] }): T {
    const returnData = {} as any;
    if (typeof dataObj === "object" && this._entityResultFieldKeysMap.size > 0) {
      Object.entries(dataObj).forEach(([key, value]) => {
        if (this._entityResultFieldKeysMap.has(key)) {
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

  private _getNativePouchId(dataId: string) {
    const { featureEntityValue } = this._getLocalVariables();
    return [featureEntityValue, dataId].join("#");
  }

  private _getBaseObject({ dataId }: { dataId: string }) {
    const { partitionKeyFieldName, sortKeyFieldName, featureEntityValue } = this._getLocalVariables();

    const dataMust = {
      _id: this._getNativePouchId(dataId),
      [partitionKeyFieldName]: dataId,
      [sortKeyFieldName]: featureEntityValue,
    };
    return dataMust;
  }

  private _withConditionPassed({ item, withCondition }: { item: any; withCondition?: IFieldCondition<T> }) {
    if (item && typeof item === "object" && withCondition?.length) {
      const isPassed = withCondition.every(({ field, equals }) => {
        return item[field] !== undefined && item[field] === equals;
      });
      return isPassed;
    }
    return true;
  }

  private _checkValidateMustBeAnObjectDataType(data: unknown) {
    if (!data || typeof data !== "object") {
      throw this._createGenericError(`Data MUST be valid object`);
    }
  }

  private _checkValidateStrictRequiredFields(onDataObj: any) {
    this._checkValidateMustBeAnObjectDataType(onDataObj);

    const { strictRequiredFields } = this._getLocalVariables();

    if (strictRequiredFields?.length) {
      for (const field of strictRequiredFields) {
        if (onDataObj[field] === null || onDataObj[field] === undefined) {
          throw this._createGenericError(`Strict required field NOT defined`);
        }
      }
    }
  }

  private _removeDuplicateString(list: string[]) {
    return Array.from(new Set(list));
  }

  private async _allHelpValidateGetValue(data: any) {
    const { error, value } = this.here_schema.validate(data, {
      stripUnknown: true,
    });

    if (error) {
      const msg = getJoiValidationErrors(error) ?? "Validation error occured";
      throw this.errorHelper.fuse_helper_createFriendlyError(msg);
    }

    return await Promise.resolve({
      validatedData: value,
    });
  }

  private _createGenericError(error: string) {
    return new GenericDataError(error);
  }

  protected async fuse_createOne({ data }: { data: T }): Promise<T> {
    this._checkValidateStrictRequiredFields(data);

    const { partitionKeyFieldName } = this._getLocalVariables();

    let dataId: string | undefined = data[partitionKeyFieldName];

    if (!dataId) {
      dataId = this._generateDynamoTableKey();
    }

    const dataMust = this._getBaseObject({ dataId });
    const fullData = { ...data, ...dataMust };

    const validated = await this._allHelpValidateGetValue(fullData);

    const result = await this._couchDbInstance().put(validated.validatedData);
    if (!result.ok) {
      throw this._createGenericError(this._operationNotSuccessful);
    }
    return this._stripNonRequiredOutputData({ dataObj: data });
  }

  protected async fuse_batchGetManyByIds({
    dataIds,
    fields,
    withCondition,
  }: {
    dataIds: string[];
    fields?: (keyof T)[] | undefined;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T[]> {
    //
    const uniqueIds = this._removeDuplicateString(dataIds);
    const fullUniqueIds = uniqueIds.map((id) => this._getNativePouchId(id));

    const data = await this._couchDbInstance().allDocs<IFullEntity<T>>({
      keys: fullUniqueIds,
      include_docs: true,
    });

    const dataList: T[] = [];

    if (withCondition?.length) {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this.here_featureEntityValue) {
          const passed = this._withConditionPassed({ item: item.doc, withCondition });
          if (passed) {
            const k = this._stripNonRequiredOutputData({
              dataObj: item.doc,
              excludeFields: fields as any[],
            });
            dataList.push(k);
          }
        }
      });
    } else {
      data?.rows?.forEach((item) => {
        if (item?.doc?.featureEntity === this.here_featureEntityValue) {
          const k = this._stripNonRequiredOutputData({
            dataObj: item.doc,
            excludeFields: fields as any[],
          });
          dataList.push(k);
        }
      });
    }
    return dataList;
  }

  protected async fuse_getManyByCondition(paramOptions: IFuseQueryParamOptions<T, any>): Promise<T[]> {
    if (!paramOptions?.query) {
      throw this._createGenericError("Invalid query object");
    }

    paramOptions.query = {
      ...paramOptions.query,
      featureEntity: this.here_featureEntityValue,
    };

    const _normalizeFields = (fields: (keyof T)[] | string[]) => {
      if (fields?.length) {
        const fieldList: string[] = [];
        for (const field of fields as string[]) {
          fieldList.push(field);
        }
        if (fieldList.length) {
          return this._removeDuplicateString(fieldList);
        }
      }
      return undefined;
    };

    const data = await this._couchDbInstance().find({
      selector: { ...paramOptions.query },
      fields: _normalizeFields(paramOptions?.fields || []),
    });
    const dataList = data?.docs?.map((item) => {
      return this._stripNonRequiredOutputData({ dataObj: item });
    });
    return dataList || [];
  }

  async fuse_getAll(): Promise<T[]> {
    const data = await this._couchDbInstance().allDocs<IFullEntity<T>>({
      include_docs: true,
      startkey: this.here_featureEntityValue,
      endkey: `${this.here_featureEntityValue}\ufff0`,
    });
    const dataList: T[] = [];
    data?.rows?.forEach((item) => {
      if (item?.doc?.featureEntity === this.here_featureEntityValue) {
        const k = this._stripNonRequiredOutputData({ dataObj: item.doc });
        dataList.push(k);
      }
    });
    return dataList;
  }

  protected fuse_getManyByConditionPaginate(
    paramOptions: IFuseQueryParamOptions<T, any>,
  ): Promise<IFusePagingResult<T[]>> {
    throw new Error("Method not implemented.");
  }

  protected fuse_getManyByIndex<TData = T, TSortKeyField = string>(
    paramOption: IFuseQuerySecondayIndexOptions<TData, TSortKeyField>,
  ): Promise<T[]> {
    throw new Error("Method not implemented.");
  }

  protected fuse_getManyByIndexPaginate<TData = T, TSortKeyField = string>(
    paramOption: IFuseQuerySecondayIndexOptions<TData, TSortKeyField>,
  ): Promise<IFusePagingResult<T[]>> {
    throw new Error("Method not implemented.");
  }

  // protected async fuse_getManyByIndexPaginate00<TData = T, TSortKeyField = string>(
  //   paramOption: IFuseQuerySecondayIndexOptions<TData, TSortKeyField>,
  // ): Promise<IFusePagingResult<T[]>> {
  //   if (!paramOption?.query) {
  //     throw new GenericDataError("Invalid query object");
  //   }
  //   if (paramOption.indexName) {
  //     throw new GenericDataError("Index MUST be defined for sort query");
  //   }

  //   paramOption.query = {
  //     ...paramOption.query,
  //     featureEntity: this.here_featureEntityValue,
  //   };

  //   const _normalizeFields = (fields: (keyof T)[] | string[]) => {
  //     if (fields?.length) {
  //       const fieldList: string[] = [];
  //       for (const field of fields as string[]) {
  //         if (field === "id") {
  //           fieldList.push("_id");
  //         } else {
  //           fieldList.push(field);
  //         }
  //       }
  //       if (fieldList.length) {
  //         return this._root_removeDuplicateString(fieldList);
  //       }
  //     }
  //     return undefined;
  //   };

  //   let paramsSort: Array<string | { [propName: string]: "asc" | "desc" }> | undefined = undefined;

  //   const indexOpt = this._indexOptions.find((f) => f.indexName === params.indexName);
  //   if (indexOpt?.fields?.length) {
  //     const query00: any = { ...paramOption.query };
  //     const query01: any = {};
  //     const query02: any = {};
  //     for (const indexOrder of indexOpt.fields) {
  //       if (query00[indexOrder] !== undefined) {
  //         query01[indexOrder] = query00[indexOrder];
  //       }
  //     }
  //     Object.entries(query00).forEach(([key, val]) => {
  //       if (query01[key] === undefined) {
  //         query02[key] = val;
  //       }
  //     });
  //     console.log({ query01, query02 });
  //     paramOption.query = { ...query01, ...query02 };
  //     //
  //     if (paramOption.pagingParams?.orderDesc) {
  //       if (Array.isArray(params.sort)) {
  //         if (params.sort.length) {
  //           paramsSort = [...params.sort] as any[];
  //         }
  //       } else {
  //         const sort00 = [...params.sort?.advanced];
  //         const sort01: Array<{ [propName: string]: "asc" | "desc" }> = [];

  //         for (const fieldName of indexOpt.fields) {
  //           const sortFind = sort00.find((f) => f.field === fieldName);
  //           if (sortFind && typeof sortFind === "object" && sortFind.field) {
  //             sort01.push({ [sortFind.field]: sortFind.sortType });
  //           }
  //         }
  //         if (sort01.length) {
  //           paramsSort = [...sort01];
  //         }
  //         console.log({ sort01 });
  //       }
  //     }
  //   }

  //   console.log({ indexName: params.indexName });
  //   const data = await this._couchDbInstance().find({
  //     selector: { ...paramOption.query },
  //     fields: _normalizeFields(paramOption.fields),
  //     limit: params.limit || undefined,
  //     skip: params.skip || undefined,
  //     sort: paramsSort,
  //     use_index: params.indexName ? params.indexName : undefined,
  //   });
  //   const dataList = data?.docs?.map((item) => {
  //     return this._root_stripNonRequiredData(item);
  //   });
  //   return dataList;
  // }

  protected async fuse_getOneById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T | null> {
    const dataInDb = await this._couchDbInstance().get<IFullEntity<T>>(dataId);
    if (!(dataInDb?._id === dataId && dataInDb.featureEntity === this.here_featureEntityValue)) {
      return null;
    }
    const passed = this._withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      return null;
    }
    return this._stripNonRequiredOutputData({ dataObj: dataInDb });
  }

  protected async fuse_updateOneById({
    dataId,
    data,
    withCondition,
  }: {
    dataId: string;
    data: T;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    const dataInDb = await this._couchDbInstance().get<IFullEntity<T>>(dataId);
    if (!(dataInDb?._id === dataId && dataInDb.featureEntity === this.here_featureEntityValue && dataInDb._rev)) {
      throw this._createGenericError("Record does not exists");
    }
    const passed = this._withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._createGenericError("Record with conditions does not exists");
    }
    const _data: IFullEntity<T> = { ...data } as any;

    const validated = await this._allHelpValidateGetValue(_data);

    const result = await this._couchDbInstance().put<T>({
      ...validated.validatedData,
      _rev: dataInDb._rev,
    });
    if (!result.ok) {
      throw this._createGenericError(this._operationNotSuccessful);
    }
    return this._stripNonRequiredOutputData({ dataObj: data });
  }

  protected fuse_updateOneDirect({ data }: { data: T }): Promise<T> {
    throw new Error("Method not implemented.");
  }

  protected async fuse_deleteById({
    dataId,
    withCondition,
  }: {
    dataId: string;
    withCondition?: IFuseFieldCondition<T> | undefined;
  }): Promise<T> {
    const dataInDb = await this._couchDbInstance().get<IFullEntity<T>>(dataId);
    if (!(dataInDb?._id === dataId && dataInDb.featureEntity === this.here_featureEntityValue)) {
      throw this._createGenericError("Record does not exists");
    }
    const passed = this._withConditionPassed({ item: dataInDb, withCondition });
    if (!passed) {
      throw this._createGenericError("Record with conditions does not exists for deletion");
    }
    const result = await this._couchDbInstance().remove(dataInDb);
    if (!result.ok) {
      throw this._createGenericError(this._operationNotSuccessful);
    }
    return this._stripNonRequiredOutputData({ dataObj: dataInDb });
  }

  protected fuse_deleteManyDangerouselyByIds({ dataIds }: { dataIds: string[] }): Promise<boolean> {
    throw new Error("Method not implemented.");
  }
}
