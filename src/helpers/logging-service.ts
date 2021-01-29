class LoggingServiceBase {
  private mode: boolean;

  constructor() {
    const mode = process.env.DYNAMODB_DATA_OPERATION_DEBUG_MODE;
    if (mode === "true") {
      this.mode = true;
    } else {
      this.mode = false;
    }
  }

  log(message: any, ...optionalParams: any[]) {
    if (this.mode) {
      console.log(message, optionalParams);
    }
  }
}

export const LoggingService = new LoggingServiceBase();
