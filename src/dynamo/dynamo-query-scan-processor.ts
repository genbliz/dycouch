import type { DynamoDB, QueryInput, QueryCommandOutput } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { IFusePagingResult } from "../type/types";
import { LoggingService } from "../helpers/logging-service";
import { FuseUtil } from "../helpers/fuse-utils";

export class DynamoQueryScanProcessor {
  //
  async fuse__helperDynamoQueryProcessor<T>({
    evaluationLimit,
    params,
    pageSize,
    lastKeyHash,
    orderDesc,
    hashKeyAndSortKey,
    dynamoDb,
  }: {
    dynamoDb: () => DynamoDB;
    evaluationLimit?: number;
    params: QueryInput;
    pageSize?: number;
    lastKeyHash?: any;
    orderDesc?: boolean;
    hashKeyAndSortKey: [string, string];
  }) {
    if (params?.ExpressionAttributeValues) {
      const marshalled = marshall(params.ExpressionAttributeValues, {
        convertEmptyValues: false,
        removeUndefinedValues: true,
      });
      params.ExpressionAttributeValues = marshalled;
    }
    const results = await this.__helperDynamoQueryScanProcessor<T>({
      dynamoDb,
      evaluationLimit,
      params,
      pageSize,
      lastKeyHash,
      orderDesc,
      hashKeyAndSortKey,
    });
    results.mainResult = this.__unmarshallToJson(results.mainResult);
    return results;
  }

  private __helperDynamoQueryScanProcessor<T>({
    evaluationLimit,
    params,
    pageSize,
    lastKeyHash,
    orderDesc,
    hashKeyAndSortKey,
    dynamoDb,
  }: {
    dynamoDb: () => DynamoDB;
    evaluationLimit?: number;
    params: QueryInput;
    pageSize?: number;
    lastKeyHash?: any;
    orderDesc?: boolean;
    hashKeyAndSortKey?: [string, string];
  }) {
    const xDefaultEvaluationLimit = 10;
    const xMinEvaluationLimit = 5;
    const xMaxEvaluationLimit = 500;

    type IResult = QueryCommandOutput | undefined;
    // type IResult = QueryCommandOutput;

    LoggingService.log({
      processorParamsInit: {
        pageSize,
        orderDesc,
        lastKeyHash,
        evaluationLimit,
        hashKeyAndSortKey,
        params,
      },
    });

    return new Promise<IFusePagingResult<T[]>>((resolve, reject) => {
      let returnedItems: any[] = [];
      let _evaluationLimit: number = 0;

      if (pageSize) {
        //
        _evaluationLimit = xDefaultEvaluationLimit;
        if (evaluationLimit) {
          _evaluationLimit = evaluationLimit;
        }

        if (_evaluationLimit < xMinEvaluationLimit) {
          _evaluationLimit = xMinEvaluationLimit;
          //
        } else if (_evaluationLimit > xMaxEvaluationLimit) {
          _evaluationLimit = xDefaultEvaluationLimit;
        }

        if (pageSize > _evaluationLimit) {
          _evaluationLimit = pageSize + 1;
          //
        } else if (pageSize === _evaluationLimit) {
          // _evaluationLimit = pageSize + 1;
        }
      }

      const queryScanUntilDone = (err: any, dataOutput: IResult) => {
        if (err) {
          LoggingService.log(err, err?.stack);
          if (returnedItems?.length) {
            resolve({ mainResult: returnedItems });
          } else {
            reject(err.stack);
          }
        } else {
          if (dataOutput?.Items?.length) {
            returnedItems = [...returnedItems, ...dataOutput.Items];
          }

          if (returnedItems?.length && hashKeyAndSortKey?.length) {
            const itemObject = returnedItems.slice(-1)[0];
            const customLastEvaluationKey = this.__createCustomLastEvaluationKey({
              itemObject,
              primaryFieldNames: hashKeyAndSortKey,
            });
            LoggingService.log({ customLastEvaluationKey });
          }

          if (pageSize && returnedItems.length >= pageSize) {
            const queryOutputResult: IFusePagingResult<T[]> = {
              mainResult: returnedItems,
            };

            if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
              const lastKeyHash = this.__encodeLastKey(dataOutput.LastEvaluatedKey);
              queryOutputResult.lastKeyHash = lastKeyHash;
            }
            resolve(queryOutputResult);
          } else if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
            //
            const _paramsDef = { ...params };
            _paramsDef.ExclusiveStartKey = dataOutput.LastEvaluatedKey;
            if (_evaluationLimit) {
              _paramsDef.Limit = _evaluationLimit;
            }

            LoggingService.log({
              dynamoProcessorParams: _paramsDef,
            });

            dynamoDb().query(_paramsDef, (err, resultData) => {
              queryScanUntilDone(err, resultData);
            });
          } else {
            resolve({ mainResult: returnedItems });
          }
        }
      };

      const _params = { ...params };
      if (_evaluationLimit) {
        _params.Limit = _evaluationLimit;
      }
      if (lastKeyHash) {
        const _lastEvaluatedKey = this.__decodeLastKey(lastKeyHash);
        if (_lastEvaluatedKey) {
          _params.ExclusiveStartKey = _lastEvaluatedKey;
        }
      }
      if (orderDesc === true) {
        _params.ScanIndexForward = false;
      }
      LoggingService.log({ dynamoProcessorParams: _params });
      dynamoDb().query(params, (err, resultData) => {
        queryScanUntilDone(err, resultData);
      });
    });
  }

  private __unmarshallToJson(items: any[]) {
    if (items?.length) {
      const itemList = items.map((item) => {
        return FuseUtil.fuse_unmarshallToJson(item);
      });
      return itemList;
    }
    return items;
  }

  private __encodeLastKey(lastEvaluatedKey: any) {
    return Buffer.from(JSON.stringify(lastEvaluatedKey)).toString("base64");
  }

  private __createCustomLastEvaluationKey({
    itemObject,
    primaryFieldNames,
  }: {
    itemObject: Record<string, any>;
    primaryFieldNames: string[];
  }) {
    const obj: Record<string, any> = {};
    primaryFieldNames.forEach((key) => {
      if (typeof itemObject[key] !== "undefined") {
        obj[key] = itemObject[key];
      }
    });
    return Object.keys(obj).length > 0 ? obj : null;
  }

  private __decodeLastKey(lastKeyHash: any) {
    let _lastEvaluatedKey: any;
    try {
      const _lastKeyHashStr = Buffer.from(lastKeyHash, "base64").toString();
      _lastEvaluatedKey = JSON.parse(_lastKeyHashStr);
    } catch (error) {
      _lastEvaluatedKey = undefined;
    }
    return _lastEvaluatedKey;
  }
}
