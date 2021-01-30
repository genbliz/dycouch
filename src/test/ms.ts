// const AWSX = require("aws-sdk");
import AWSX from "aws-sdk";

const patients = [{ name: "Madu", hisList: ["fizz", "buzz", "pop"], hisSet: new Set(["fizz", "buzz", "pop"]) }];

// const marshalledData = DynamoDBV2.Converter.marshall(data);
const marshalled = AWSX.DynamoDB.Converter.marshall(patients[0]);

const unmarshalled = AWSX.DynamoDB.Converter.unmarshall(marshalled);

console.log(JSON.stringify({ marshalled, unmarshalled }, null, 2));
process.exit(0);
