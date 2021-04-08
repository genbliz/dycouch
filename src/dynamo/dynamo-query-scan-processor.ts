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
    partitionAndSortKey,
    dynamoDb,
  }: {
    dynamoDb: () => DynamoDB;
    evaluationLimit?: number;
    params: QueryInput;
    pageSize?: number;
    lastKeyHash?: any;
    orderDesc?: boolean;
    partitionAndSortKey: [string, string];
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
      partitionAndSortKey,
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
    partitionAndSortKey,
    dynamoDb,
  }: {
    dynamoDb: () => DynamoDB;
    evaluationLimit?: number;
    params: QueryInput;
    pageSize?: number;
    lastKeyHash?: any;
    orderDesc?: boolean;
    partitionAndSortKey: [string, string];
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
        partitionAndSortKey,
        params,
      },
    });

    return new Promise<IFusePagingResult<T[]>>((resolve, reject) => {
      let returnedItems: any[] = [];
      let evaluationLimit01: number = 0;

      if (pageSize) {
        //
        evaluationLimit01 = xDefaultEvaluationLimit;
        if (evaluationLimit) {
          evaluationLimit01 = evaluationLimit;
        }

        if (evaluationLimit01 < xMinEvaluationLimit) {
          evaluationLimit01 = xMinEvaluationLimit;
          //
        } else if (evaluationLimit01 > xMaxEvaluationLimit) {
          evaluationLimit01 = xDefaultEvaluationLimit;
        }

        if (pageSize > evaluationLimit01) {
          evaluationLimit01 = pageSize + 1;
          //
        } else if (pageSize === evaluationLimit01) {
          // _evaluationLimit = pageSize + 1;
        }
      }

      const queryScanUntilDone = (err: any, dataOutput: IResult) => {
        if (err) {
          LoggingService.log(err, err?.stack);
          if (returnedItems?.length) {
            resolve({
              mainResult: returnedItems,
              lastKeyHash: undefined,
            });
          } else {
            reject(err.stack);
          }
        } else {
          if (dataOutput?.Items?.length) {
            returnedItems = [...returnedItems, ...dataOutput.Items];
          }

          if (pageSize && returnedItems.length >= pageSize) {
            const queryOutputResult: IFusePagingResult<T[]> = {
              mainResult: returnedItems,
              lastKeyHash: undefined,
            };

            if (partitionAndSortKey?.length === 2 && returnedItems.length > pageSize) {
              //
              queryOutputResult.mainResult = returnedItems.slice(0, pageSize);
              const itemObject = queryOutputResult.mainResult.slice(-1)[0];
              const customLastEvaluationKey = this.__createCustomLastEvaluationKey({
                itemObject,
                partitionAndSortKey,
              });
              //
              LoggingService.log({ customLastEvaluationKey });
              queryOutputResult.lastKeyHash = this.__encodeLastKey(customLastEvaluationKey);
              //
            } else if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
              queryOutputResult.lastKeyHash = this.__encodeLastKey(dataOutput.LastEvaluatedKey);
            }

            resolve(queryOutputResult);
            //
          } else if (dataOutput?.LastEvaluatedKey && Object.keys(dataOutput.LastEvaluatedKey).length) {
            //
            const paramsDef01 = { ...params };
            //
            paramsDef01.ExclusiveStartKey = dataOutput.LastEvaluatedKey;
            if (evaluationLimit01) {
              paramsDef01.Limit = evaluationLimit01;
            }

            LoggingService.log({ dynamoProcessorParams: paramsDef01 });

            dynamoDb().query(paramsDef01, (err, resultData) => {
              queryScanUntilDone(err, resultData);
            });
          } else {
            resolve({
              mainResult: returnedItems,
              lastKeyHash: undefined,
            });
          }
        }
      };

      const params01 = { ...params };

      if (evaluationLimit01) {
        params01.Limit = evaluationLimit01;
      }

      if (lastKeyHash) {
        const lastEvaluatedKey01 = this.__decodeLastKey(lastKeyHash);
        if (lastEvaluatedKey01) {
          params01.ExclusiveStartKey = lastEvaluatedKey01;
        }
      }

      if (orderDesc === true) {
        params01.ScanIndexForward = false;
      }

      LoggingService.log({ dynamoProcessorParams: params01 });
      //
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
    partitionAndSortKey,
  }: {
    itemObject: Record<string, any>;
    partitionAndSortKey: [string, string];
  }) {
    const obj: Record<string, any> = {};
    partitionAndSortKey.forEach((key) => {
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
