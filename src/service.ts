import { S3 } from 'aws-sdk'
import { parseAsync } from 'json2csv';
import csv = require('csvtojson');

const s3ParseUrl = require('s3-url-parser');

export async function splitter(url: string) {
    try {
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
        return "splitted to .csv files";
    } catch (err) {
        return err.toString();
    }
}