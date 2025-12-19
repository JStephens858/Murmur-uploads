import fastify from "fastify";
import { Throttle } from 'stream-throttle';
import fs from 'fs';
import cors from '@fastify/cors';
import { Server } from "@tus/server";
import { FileStore } from "@tus/file-store";

// Max upload speed in bytes per second (e.g., 1048576 = 1 MB/s, 524288 = 512 KB/s)
// Set to null or 0 to disable throttling
const MAX_UPLOAD_SPEED = 1048576; // 1 MB/s default

const app = fastify({ 
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
	onIncomingRequest: (req, res) => {
		// Apply upload speed throttling if configured
		if (MAX_UPLOAD_SPEED && MAX_UPLOAD_SPEED > 0) {
			const throttle = new Throttle({ rate: MAX_UPLOAD_SPEED });
			req.pipe(throttle);
			return throttle;
		}
		return req;
	},
	/*
	onUploadCreate: async (req, res, upload) => {
    console.log('=== DEBUGGING TUS UPLOAD CREATE ===');
    console.log('Headers received:', {
      'x-forwarded-proto': req.headers['x-forwarded-proto'],
      'x-forwarded-host': req.headers['x-forwarded-host'],
      'forwarded': req.headers['forwarded'],
      'host': req.headers['host']
    });
    console.log('Upload ID:', upload.id);
    console.log('================================');
    return res;
  }
  */
});
let port = 4000;

app.addContentTypeParser('*', function (request, payload, done) {
  done(null, payload);
});
/*
app.addContentTypeParser(
	"application/offset+octet-stream",
	(request, payload, done) => done(null)
);
*/
app.all("/files", async(req, res) => {

	req.raw.on('data', (chunk) => {
		console.log(`Received ${chunk.length} bytes`);
//		chunks.push(chunk);
		// Do something with each chunk here
	});

	await tusServer.handle(req.raw, res.raw);
});
app.all("/files/*", (req, res) => {
	console.log(req.url);
	console.log(req.headers);
	tusServer.handle(req.raw, res.raw);
});


try {
	await app.listen({ host:"0.0.0.0", port });
	console.log(`Server listening on port ${port}`);
} catch (err) {
	app.log.error(err);
	process.exit(1);
}

async function delay(ms) {
	// return await for better async stack trace support in case of errors.
	return await new Promise(resolve => setTimeout(resolve, ms));
}
