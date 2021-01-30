export class GenericDataError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class GenericFriendlyError extends Error {
  constructor(message: string) {
    super(message);
  }
}

export class FuseErrorUtils {
  fuse_helper_validateRequiredNumber(keyValueValidates: { [key: string]: number }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(!isNaN(Number(value)) && typeof value === "number")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new GenericFriendlyError(`${errors.join("; ")}.`);
    }
  }

  fuse_helper_createFriendlyError(message: string, statusCode?: number) {
    return new GenericFriendlyError(message);
  }

  fuse_helper_validateRequiredString(keyValueValidates: { [key: string]: string }) {
    const errors: string[] = [];
    Object.entries(keyValueValidates).forEach(([key, value]) => {
      if (!(value && typeof value === "string")) {
        errors.push(`${key} is required`);
      }
    });
    if (errors.length) {
      throw new GenericFriendlyError(`${errors.join("; ")}.`);
    }
  }
}
