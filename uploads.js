import dotenv from 'dotenv';
import pino from 'pino';
import fastify from "fastify";
import { Throttle } from 'stream-throttle';
import fs from 'fs';
import cors from '@fastify/cors';
import { Server } from "@tus/server";
import { FileStore } from "@tus/file-store";
import {EVENTS, ERRORS, REQUEST_METHODS, TUS_RESUMABLE, HEADERS} from '@tus/utils'
import MurmurAPI from '../Murmur-apollo/datasources/murmurAPI.js';
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import redis from "redis";


import { gql } from 'graphql-tag';
import ApolloBoost from 'apollo-boost';
const ApolloClient = ApolloBoost.ApolloClient;
import { fetch } from 'cross-fetch';
import { createHttpLink } from  'apollo-link-http';
import { InMemoryCache } from 'apollo-cache-inmemory';
const client = new ApolloClient({
        link: createHttpLink({
                uri: process.env.MURMUR_API_SERVER,
                fetch: fetch
        }),
        cache: new InMemoryCache()
});

dotenv.config();

const murmurDbConfig = {
    host : process.env.DB_HOST,
    user : process.env.DB_USER,
    password : process.env.DB_PASS,
    database : 'murmur',
    enableKeepAlive : true,
    keepAliveInitialDelay: 10000,
    trackDBConnections: false
};
if (process.env.DB_ENCRYPTION == "true"){
    murmurDbConfig.ssl = {
        ca: fs.readFileSync('../Murmur-common/certs/us-west-2-bundle.pem'),
    }
}

var credentials = {
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY
};
const AWSS3Client = new S3Client({ credentials: credentials, region: 'us-west-2' });

const murm = new MurmurAPI(murmurDbConfig);

const publishRedisClient = redis.createClient({
  socket: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    tls: process.env.REDIS_USE_TLS == 1 ? true : false
  },
  password: process.env.REDIS_PASSWORD
});

publishRedisClient.on('error', err => {
    console.log('Publish Redis Error ' + err);
});


// Max upload speed in bytes per second (e.g., 1048576 = 1 MB/s, 524288 = 512 KB/s)
// Set to null or 0 to disable throttling
const MAX_UPLOAD_SPEED = 1048576; // 1 MB/s default

const app = fastify({ 
	//logger: {level: 'error'},
	logger: true,
	trustProxy: true,
	https: {
		key: fs.readFileSync('/etc/letsencrypt/live/up01.murmurmd.com/privkey.pem'),
		cert: fs.readFileSync('/etc/letsencrypt/live/up01.murmurmd.com/cert.pem')
	}
});

await app.register(cors, {
  origin: true, // or specify your iOS app's origin
  credentials: true,
  exposedHeaders: [
    'Upload-Offset',
    'Upload-Length',
    'Tus-Resumable',
    'Upload-Metadata',
    'Upload-Defer-Length',
    'Upload-Concat',
    'Location'
  ],
  allowedHeaders: [
    'Upload-Offset',
    'Upload-Length',
    'Tus-Resumable',
    'Upload-Metadata',
    'Upload-Defer-Length',
    'Upload-Concat',
    'Content-Type',
    'Authorization'
  ]
});

const tusServer = new Server({
	path: "/files",
	datastore: new FileStore({ directory: "./files" }),
	respectForwardedHeaders: true,
	generateUrl: (req, { proto, host, path, id }) => {
    // Override proto to always be https
		console.log(`generating: https://${host}${path}/${id}`);
    return `https://${host}${path}/${id}`;
  },
	// HOOKS - These run during the request lifecycle

  // 1. onIncomingRequest - Runs BEFORE all handlers (access control, auth)
  async onIncomingRequest(req, uploadId) {
    // Access control / authentication
	  /*
    const token = req.headers.authorization;
    if (!token) {
      throw { status_code: 401, body: 'Unauthorized' };
    }
*/
    // You can verify JWT, check permissions, etc.
    console.log(`Request for upload: ${uploadId}`);
  },

  // 2. onUploadCreate - Runs BEFORE a new upload is created
  async onUploadCreate(req, upload) {
    // Validate metadata
    const metadata = upload.metadata;

	  console.log("metadata");
	  console.log(metadata);
	  console.log("_______________");
    if (!metadata.filename) {
      throw {
        status_code: 400,
        body: 'filename required in Upload-Metadata'
      };
    }
    console.log(`Upload create filename: ${metadata.filename}`);

    // You can modify/add metadata by returning it
    return {
      metadata: {
        ...upload.metadata,
        userId: req.headers['x-user-id'], // Add custom metadata
        uploadedAt: new Date().toISOString()
      }
    };
  },

  // 3. onUploadFinish - Runs AFTER upload completes
  async onUploadFinish(req, res, upload) {
    console.log(`Upload finished: ${upload}`);

    // Do post-processing: move file, trigger webhook, etc.
    // Example: notify your app that upload is done
//    await notifyUploadComplete(upload);
  },

  // 4. onResponseError - Runs when an error response is about to be sent
  async onResponseError(req, err) {
    console.error('TUS error:', err);

    // Map custom errors or do observability
    // You can return custom error response
    return {
      status_code: err.status_code || 500,
      body: err.message || 'Upload error'
    };
  }

});

tusServer.on(EVENTS.POST_CREATE, (req, res, upload) => {
  console.log('---- Upload created:', upload);
});

tusServer.on(EVENTS.POST_RECEIVE, (req, res, upload) => {
  console.log('---- Chunk received for:', upload);
});

tusServer.on(EVENTS.POST_FINISH, async(req, res, upload) => {
  /*
  Upload {
  id: '34cca789c72da3f5cfc32e5d4ae1f65d',
  metadata: {
    postDirectory: 'toplevel-edf7f448-842e-4f20-bc34-6dd72927ec19',
    fileKey: '465c8dcf-b63f-4cfd-afab-892cf6e4dca5.jpeg',
    postId: 'f0ed8278-e6a4-434a-8f3e-abd8e8a5a0bc',
    filename: 'F65FC80A-9361-4B55-B16F-586838E27FE44efb91e1-411d-4819-920b-484c14e0f4b3-preview.jpg',
    filetype: 'image/jpeg',
    mediaElementId: 'f4d6bada-8ec6-4f6d-867b-7018ab404a74',
    uploadedAt: '2025-12-31T17:24:06.776Z'
  },
  size: 77836,
  offset: 77836,
  creation_date: '2025-12-31T17:24:06.776Z',
  storage: { type: 'file', path: './files/34cca789c72da3f5cfc32e5d4ae1f65d' }
}
*/
  console.log('---- Upload finished:', upload);
  if (upload?.id && upload?.metadata?.fileKey){
    upload.hostname = process.env.HOSTNAME_FOR_HEALTHCHECK;
    await murm.query("update uploadingFiles set uploadServer=?,uploadId=? where fileKey=?",[process.env.HOSTNAME_FOR_UPLOADS, upload.id,upload.metadata.fileKey]);
    await publishRedisClient.publish('redis_UPLOAD_FINISHED',JSON.stringify(upload));
  }
});

tusServer.on(EVENTS.POST_TERMINATE, (req, res, uploadId) => {
  console.log('---- Upload terminated:', uploadId);
});


app.addContentTypeParser('*', function (request, payload, done) {
  done(null, payload);
});
/*
app.addContentTypeParser(
	"application/offset+octet-stream",
	(request, payload, done) => done(null)
);
*/
app.all("/health-check-*", async(req, res) => {
	var service = req.url.split('-')[2];
	try{
		//Let's do it this way because then it checks whether the API server is running and whether we're supposed to be running
		var result = await fireHealthCheckToAPI({service, hostname:process.env.HOSTNAME_FOR_HEALTHCHECK});
		
		if (result == true){
			res.code(200).send();
		}else{
			res.code(500).send();
		}
	}catch(e){
		res.code(500).send();
	}
});
app.all("/files", async(req, res) => {

	console.log(req.url);
	console.log(req.headers);
	await tusServer.handle(req.raw, res.raw);
});
app.all("/files/*", (req, res) => {
	console.log(req.url);
	console.log(req.headers);
	tusServer.handle(req.raw, res.raw);
});

async function fireHealthCheckToAPI({service, hostname}){
        try {
                var result = await client.query({
                        query: gql`query healthCheck($hostname: String, $service: String) {
                                healthCheck(hostname:$hostname, service:$service) {
                                        success
                                }
                        }`,
                        variables: {
				service,
				hostname
                        },
                        context: {
                                headers: {
                                        authorization: process.env.HEALTH_CHECK_AUTH_TOKEN
                                }
                        }
                })
		return result?.data?.healthCheck?.success;
        }catch (e){
                console.log(e);
        }
}

let port = process.env.UPLOADS_SERVICE_PORT;
(async() => {

	await publishRedisClient.connect();
	try {
		await app.listen({ host:"0.0.0.0", port });
		console.log(`Server listening on port ${port}`);
	} catch (err) {
		app.log.error(err);
		process.exit(1);
	}
})();

async function delay(ms) {
	// return await for better async stack trace support in case of errors.
	return await new Promise(resolve => setTimeout(resolve, ms));
}
