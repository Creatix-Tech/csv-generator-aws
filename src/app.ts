import * as bodyParser from 'body-parser';
import { Response, Request } from "express";
import * as express from "express";
import serverless = require('serverless-http');

import { splitter } from './service';

export const app = express();

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.post('/', async (req: Request, res: Response) => {
    const url = (req.body as { url: string }).url;
    try {
        const response: string = await splitter(url);
        res.json(response);
    } catch (err) {
        res.json(err.toString());
    }
});

export const handler = serverless(app);

