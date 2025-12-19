import fastify from "fastify";
import { Throttle } from 'stream-throttle';
import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';

// Directory to store uploaded files
const FILES_DIR = "/home/ec2-user/Murmur-uploads/files";

// Ensure files directory exists
if (!fs.existsSync(FILES_DIR)) {
  fs.mkdirSync(FILES_DIR, { recursive: true });
}

const app = fastify({
        logger: true,
        trustProxy: true,
        https: {
                key: fs.readFileSync('/etc/letsencrypt/live/up01.murmurmd.com/privkey.pem'),
                cert: fs.readFileSync('/etc/letsencrypt/live/up01.murmurmd.com/cert.pem')
        }
});

app.addContentTypeParser('*', function (request, payload, done) {
  done(null, payload);
});
// Handle PUT requests for file uploads
// The filename is captured from the URL path
app.put('/:filename', async (request, reply) => {
  console.log("PUT request");
  const { filename } = request.params;
  
  // Sanitize filename to prevent directory traversal
  const sanitizedFilename = path.basename(filename);
  const filePath = path.join(FILES_DIR, sanitizedFilename);
  
  console.log(filePath);

  try {
    // Create write stream for the file
    const writeStream = fs.createWriteStream(filePath);
    
    // Stream the request body directly to the file
    // This processes the upload in chunks
    await pipeline(request.raw, writeStream);
    
    reply.code(201).send({
      success: true,
      message: 'File uploaded successfully',
      filename: sanitizedFilename,
      path: filePath
    });
  } catch (error) {
    request.log.error(error);
    reply.code(500).send({
      success: false,
      message: 'Error uploading file',
      error: error.message
    });
  }
});

// Optional: GET endpoint to retrieve files
app.get('/:filename', async (request, reply) => {
  const { filename } = request.params;
  const sanitizedFilename = path.basename(filename);
  const filePath = path.join(FILES_DIR, sanitizedFilename);
  console.log(filePath);
  
  if (!fs.existsSync(filePath)) {
    return reply.code(404).send({
      success: false,
      message: 'File not found'
    });
  }
  
  // Stream the file back to the client
  const readStream = fs.createReadStream(filePath);
  reply.type('application/octet-stream').send(readStream);
});

// Health check endpoint
app.get('/', async (request, reply) => {
  return {
    status: 'running',
    message: 'File upload server is running',
    usage: 'PUT /:filename to upload, GET /:filename to download'
  };
});

// Start the server
const start = async () => {
  try {
    await app.listen({ port: 4000, host: '0.0.0.0' });
    console.log('Server is running on http://localhost:4000');
    console.log(`Files will be stored in: ${FILES_DIR}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }
};

start();
