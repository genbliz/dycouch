import { BaseRepository } from "./base-repo";
import Joi from "joi";
import { IFuseQueryDefinition } from "../";

export interface IPayment {
  amount: number;
  category: string;
  invoiceId: string;
  transactionId?: string;
  remark: string;
}

// const query: IQueryDefinition<IPayment> = { amount: 0, category: "" };
// if (query) {
//   //
// }

const _searchTerm = "";

export const paramOptions: IFuseQueryDefinition<IPayment> = {
  $or: [
    { amount: { $contains: _searchTerm } },
    { category: { $contains: _searchTerm } },
    { invoiceId: { $contains: _searchTerm } },
    { transactionId: { $contains: _searchTerm } },
    { remark: { $contains: _searchTerm } },
  ],
};

const schemaSubDef = {
  category: Joi.string().required(),
  amount: Joi.number().min(1),
  invoiceId: Joi.string().empty("").default(null).allow(null),
  transactionId: Joi.string().empty("").default(null).allow(null),
  remark: Joi.string().empty("").default(null).allow(null),
};

const getRandom = () =>
  [
    //
    Math.round(Math.random() * 99999),
    Math.round(Math.random() * 88),
    Math.round(Math.random() * 99),
  ].reduce((prev, cur) => prev + cur, 0);

class MyRepositoryBase extends BaseRepository<IPayment> {
  constructor() {
    super({
      schemaSubDef,
      secondaryIndexOptions: [],
      featureEntityValue: "table_users",
    });
  }

  getIt() {
    return this.fuse_getManyByCondition({
      partitionKeyQuery: {
        equals: 0,
      },
      fields: ["amount"],
      query: {
        amount: 9,
        category: { $gt: "" },
      },
    });
  }

  async create() {
    await this.fuse_updateOneById({
      dataId: "",
      data: {
        amount: getRandom(),
        category: getRandom().toString(),
        invoiceId: getRandom().toString(),
        remark: getRandom().toString(),
        transactionId: getRandom().toString(),
      },
      withCondition: [
        {
          field: "invoiceId",
          equals: "",
        },
      ],
    });
  }
}

export const MyRepository = new MyRepositoryBase();

let count = 0;

function runInserts() {
  const tout = setInterval(() => {
    count++;
    console.log({ count });
    if (count >= 10) {
      clearInterval(tout);
      process.exit(0);
    } else {
      MyRepository.create().catch((e) => console.log(e));
    }
  }, 2000);
}

setTimeout(() => {
  console.log("Initializing");
  runInserts();
}, 10);

// ts-node ./src/test/repo.ts
