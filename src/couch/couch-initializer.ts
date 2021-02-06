import type { IFuseCoreEntityModel } from "../core/base-schema";
import Nano from "nano";

type IBaseDef<T> = Omit<T & IFuseCoreEntityModel, "">;

interface IOptions {
  //http://admin:mypassword@localhost:5984
  couchConfig: {
    /** eg: ```127.0.0.1, localhost, example.com```  */
    host: string;
    password?: string;
    username?: string;
    databaseName: string;
    port?: number;
    /** default: ```http``` */
    protocol?: "http" | "https";
  };
  // sqliteConfig?: {
  //   dbDirectory?: string;
  //   canSplitDb?: boolean;
  // };
}

export class FuseInitializerCouch {
  private _databaseInstance!: Nano.DocumentScope<any>;

  private readonly couchConfig: IOptions["couchConfig"];
  // private readonly sqliteConfig: IOptions["sqliteConfig"];
  // readonly sqliteSplitDb: boolean;

  constructor({ couchConfig }: IOptions) {
    this.couchConfig = couchConfig;
  }

  private getFullDbUrl(config: IOptions["couchConfig"]) {
    //http://admin:mypassword@localhost:5984
    const protocol = config?.protocol || "http";
    const dbUrlPart: string[] = [`${protocol}://`];

    if (config?.username && config.password) {
      dbUrlPart.push(config.username);
      dbUrlPart.push(`:${config.password}@`);
    }

    dbUrlPart.push(config.host);

    if (config?.port) {
      dbUrlPart.push(`:${config.port}`);
    }
    return dbUrlPart.join("");
  }

  getInstance<T>(dbName: string): Nano.DocumentScope<IBaseDef<T>> {
    if (!this._databaseInstance) {
      const n = Nano(this.getFullDbUrl(this.couchConfig));
      const db = n.db.use<IBaseDef<T>>(this.couchConfig.databaseName);
      this._databaseInstance = db;
    }
    return this._databaseInstance;
  }
}
