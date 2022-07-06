import { S3 } from 'aws-sdk'
import { parseAsync } from 'json2csv';
import csv = require('csvtojson');
import { APIGatewayEvent } from 'aws-lambda';

const s3ParseUrl = require('s3-url-parser');

export const handler = async function (event: APIGatewayEvent) {
    try {
        let url: string;
        if (event.body !== null && event.body !== undefined) {
            let body = JSON.parse(event.body)
            if (body.url)
                url = body.url;
        }
        if (url) {
            return {
                statusCode: 401,
                body: "Url Required"
            }
        }
        const s3 = new S3();
        let params: { Bucket: string, Key: string };
        const splitMap: Map<string, Object[]> = new Map();

        const { bucket, key } = s3ParseUrl(url);
        params = {
            Bucket: bucket,
            Key: key,
        }
        const csvString = (await (s3.getObject(params).promise()))
            .Body.toString();
        const jsonFromCSV: JSON[] = await csv().fromString(csvString);

        jsonFromCSV.forEach(document => {
            const newObj: { [key: string]: string } = {};
            let restEntries: [string, string][] = [];
            let documentEntries: [string, string][] = Object.entries(document);
            const fileName = documentEntries[0][1];

            if (!splitMap.has(fileName)) {
                splitMap.set(fileName, []);
            }
            for (let i = 1; i < documentEntries.length; i++) {
                restEntries.push(documentEntries[i]);
            }

            restEntries.forEach(pair => {
                newObj[pair[0]] = pair[1];
            });
            splitMap.get(fileName).push(newObj);
        });

        for (let fileName of splitMap.keys()) {
            const csvv = await parseAsync(splitMap.get(fileName));
            const putParams = {
                Body: csvv,
                Bucket: bucket,
                Key: fileName + ".csv"
            }
            await s3.putObject(putParams).promise();
        }
        return {
            statusCode: 200,
            body: "splitted to .csv files"
        };
    } catch (err) {
        return {
            statusCode: 500,
            body: err.toString()
        };
    }
}