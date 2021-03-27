import Joi from "joi";

export interface IFuseCoreEntityModel {
  id: string;
  featureEntity: string;
}

export const coreSchemaDefinition = {
  id: Joi.string().required().min(5).max(250),
  featureEntity: Joi.string().required().min(2).max(250),
} as const;
