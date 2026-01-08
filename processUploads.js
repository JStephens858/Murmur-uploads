import dotenv from 'dotenv';
import fs from 'fs';
import { exec } from 'child_process';
import MurmurAPI from '../Murmur-apollo/datasources/murmurAPI.js';
import { MurmurApiAwsBuckets } from '../Murmur-apollo/datasources/MurmurConstants.js';
import needle from 'needle';
import { fromIni } from "@aws-sdk/credential-providers";
import { getSignedUrl, S3RequestPresigner } from "@aws-sdk/s3-request-presigner";
import { PutObjectCommand, GetObjectCommand, S3, S3Client, S3ServiceException } from "@aws-sdk/client-s3";
import { Hash } from "@smithy/hash-node";
//import { v7 as uuidv7 } from "uuid";
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

const s3Region = "us-west-2";
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

const s3Client = new S3Client({
//        credentials: fromIni({profile: "s3_profile",}),
        region: s3Region,
        sha256: Hash.bind(null, "sha256"),
});
const s3Presigner = new S3RequestPresigner({
        ...s3Client.config
});

const subscribeRedisClient = redis.createClient({
  socket: {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    tls: process.env.REDIS_USE_TLS == 1 ? true : false
  },
  password: process.env.REDIS_PASSWORD
});

subscribeRedisClient.on('error', err => {
    console.log('Subscribe Redis Error ' + err);
});

const murm = new MurmurAPI(murmurDbConfig);

async function getFinishedUpload() {
	await murm.query("update uploadingFiles set serverHandlingProcessing=?,serverClaimedDate=now() where uploadServer = ? and serverHandlingProcessing is null order by createdDate limit 1",[process.env.HOSTNAME_FOR_UPLOADS, process.env.HOSTNAME_FOR_UPLOADS]);
	var [rows] = await murm.query("select * from uploadingFiles where serverHandlingProcessing=? and completedDate is null",[process.env.HOSTNAME_FOR_UPLOADS]);

	/*
	if (rows.length > 1){
		try{
			await murm.query("insert into serverErrors (hostname,eventId,method,args,username,name,message,extensions,stack) values(?,?,?,?,?,?,?,?,?)",[process.env.HOSTNAME_FOR_UPLOADS, "0", "getFinishedUpload", "{}", "uploads", "Too Many", "Got more than 1 file ready", "", ""]);
		} catch(err){
			console.log(err);
		};
	}
	*/
	return rows;
}

async function attemptProcessing(){
	var finishedUploads = await getFinishedUpload();
	while (finishedUploads.length > 0){
		for (var upload of finishedUploads){
			await processUpload(upload);
		}
		finishedUploads = await getFinishedUpload();
	}
}

async function execShellCommand(cmd) {
	return await new Promise((resolve, reject) => {
		exec(cmd, (error, stdout, stderr) => {
			if (error) {
				console.warn(error);
			}
			resolve(stdout? stdout : stderr);
		});
	});
}

async function deleteFile(filename){
        try{
                fs.unlinkSync(filename);
        }catch(e){}
        return true;
}

async function preprocessPreviewImageUpload({upload, localFile}){
	
	var command = "ffprobe -v quiet -print_format json -show_format -show_streams -show_error -show_chapters " + localFile;
	var data = await execShellCommand(command);
	var probeObj = JSON.parse(data);
	upload.duration = 0;
	upload.width = 0;
	upload.height = 0;
	if (probeObj.error && probeObj.error.code != 0){
	}else{
		if (probeObj?.streams?.length){
			for (var stream of probeObj.streams){
				if (stream.width){
					upload.width = stream.width;
				}
				if (stream.height){
					upload.height = stream.height;
				}
			}
		}
		const previewImageRequest = JSON.stringify({
			bucket: MurmurApiAwsBuckets[upload.uploadType],
			key: upload.fileKey,
			"edits": {
				"resize": {
					"width": 600,
					"height": 600,
					"fit": "inside"
				}
			}
		});
		upload.mediaPreviewImageUrl = process.env.CLOUDFROUNT_PREFIX_FOR_POST_IMAGES + btoa(previewImageRequest);
	}
}


async function preprocessImageUpload({upload, localFile}){
	
	var command = "ffprobe -v quiet -print_format json -show_format -show_streams -show_error -show_chapters " + localFile;
	var data = await execShellCommand(command);
	var probeObj = JSON.parse(data);
	upload.duration = 0;
	upload.width = 0;
	upload.height = 0;
	if (probeObj.error && probeObj.error.code != 0){
	}else{
		if (probeObj?.streams?.length){
			for (var stream of probeObj.streams){
				if (stream.width){
					upload.width = stream.width;
				}
				if (stream.height){
					upload.height = stream.height;
				}
			}
		}
		const previewImageRequest = JSON.stringify({
			bucket: MurmurApiAwsBuckets[upload.uploadType],
			key: upload.fileKey,
			"edits": {
				"resize": {
					"width": 600,
					"height": 600,
					"fit": "inside"
				}
			}
		});
		upload.mediaPreviewImageUrl = process.env.CLOUDFROUNT_PREFIX_FOR_POST_IMAGES + btoa(previewImageRequest);
	}
}


async function preprocessVideoUpload({upload, localFile}){
	//This will get the duration and frame size of the video, and will generate a posterframe.
	
	var command = "ffprobe -v quiet -print_format json -show_format -show_streams -show_error -show_chapters " + localFile;
	//console.log(command);
	var data = await execShellCommand(command);
	var probeObj = JSON.parse(data);
	upload.duration = 0;
	upload.width = 0;
	upload.height = 0;
	var timestamp = "00:00:00";
	var channel = "0:2";
	if (probeObj.error && probeObj.error.code != 0){
	}else{
		if (probeObj?.streams?.length){
			for (var stream of probeObj.streams){
				if (stream.width){
					upload.width = stream.width;
				}
				if (stream.height){
					upload.height = stream.height;
				}
			}
		}
		if (probeObj?.format?.duration){
			var previewSecs = 0;
			upload.duration = parseInt(probeObj.format.duration*1000);
			if (upload.previewImagePercent){
				previewSecs = probeObj.format.duration * upload.previewImagePercent;
				if (previewSecs < 4){
					timestamp = "00:00:03";
				}else{
					timestamp = previewSecs;
				}
			}
		}
		var parts = upload.fileKey.split('.');
		if (parts.length > 1){
			parts.splice(-1, 1);
		}
		var fileBase = parts.join('_');
		var localImageFile = `posterFrames/${fileBase}.jpeg`;
		var videoUrl = `files/${upload.uploadId}`;
		var previewImageCommand = "ffmpeg -y -i \"" + videoUrl + "\" -ss " + timestamp + " -frames:v 1 " + localImageFile;
		await deleteFile(localImageFile);
		var previewResult = await execShellCommand(previewImageCommand);
		const stats = fs.statSync(localImageFile);
		if (stats.size){
			const commandParams = {
				ContentType: "image/jpeg",
				Bucket: MurmurApiAwsBuckets["postVideo"],
				Key: localImageFile,
			};
			const command = new PutObjectCommand(commandParams);
			var presignedPUTURL = null;
			try {
				presignedPUTURL = await getSignedUrl(s3Client, command, { expiresIn: 600000 });
			}catch(err) {
					console.log(err);
			}
			var needleParams = { json: true, headers:{'Content-type': 'image/jpeg', 'Content-Length': stats.size}};
			var result = await needle('put', presignedPUTURL, fs.createReadStream(localImageFile), needleParams)
			if (result.statusCode == 200){
				upload.previewImageFileKey = localImageFile;
				upload.bucket = MurmurApiAwsBuckets["postVideo"];

				const previewImageRequest = JSON.stringify({
					bucket: MurmurApiAwsBuckets[upload.uploadType],
					key: localImageFile,
					"edits": {
						"resize": {
							"width": 600,
							"height": 600,
							"fit": "inside"
						}
					}
				});
				upload.mediaPreviewImageUrl = process.env.CLOUDFROUNT_PREFIX_FOR_POST_IMAGES + btoa(previewImageRequest);
			}
		}
	}
}

async function processUpload(upload){
	console.log(`processing: ${upload.uploadId}`);
	try {
		var bucket = MurmurApiAwsBuckets[upload.uploadType];
		var originalFilename = upload.originalFilename;
		var localFile = `files/${upload.uploadId}`;

		console.log("upload");
		console.log(upload);
		console.log("______________");
		if (upload.uploadType=='postVideo'){
			var previewResult = await preprocessVideoUpload({upload, localFile});
		}else if (upload.uploadType=='postImage'){
			var previewResult = await preprocessImageUpload({upload, localFile});
		}else if (upload.uploadType=='postPreviewImage'){
			var previewResult = await preprocessPreviewImageUpload({upload, localFile});
		}


		var ext = upload.fileKey.split('.').pop().toLowerCase();
		if (originalFilename){
			originalFilename.split('.').pop().toLowerCase();
		}
		const contentDisposition = `attachment; filename="${originalFilename}"`;

		const stats = fs.statSync(localFile);
		const commandParams = {
			ContentType: upload.contentType,
			Bucket: bucket,
			Key: upload.fileKey,
		};
		if (originalFilename){
			commandParams.ContentDisposition = contentDisposition;
		}
		const command = new PutObjectCommand(commandParams);
		var presignedPUTURL = null;
		try {
			presignedPUTURL = await getSignedUrl(s3Client, command, { expiresIn: 600000 });
		}catch(err) {
			console.log(err);
		}
		var needleParams = { json: true, headers:{'Content-type': upload.contentType, 'Content-Length': stats.size}};
		if (originalFilename){
			needleParams.headers["Content-Disposition"] = contentDisposition;
		}
		var result = await needle('put', presignedPUTURL, fs.createReadStream(localFile), needleParams)
		console.log("status: " + result.statusCode);
		if (result.statusCode == 200){
			await murm.query("update uploadingFiles set bucket=?,completedDate=now() where fileKey=?",[bucket,upload.fileKey]);
			await fireFileCompletedRequest({
				"bucketName":bucket,
				"fileKey":upload.fileKey,
				"size":upload.size,
				"previewImageFileKey":upload.previewImageFileKey,
				"generatedPreviewUrl":upload.mediaPreviewImageUrl,
				"duration":upload.duration,
				"width":upload.width,
				"height":upload.height,
			});
		}
	} catch (err) {
		console.log(err);
		var username = "unknown";
		var jsonExtensions = "";
		var jsonArgs = "";
		var stack = "";

		try{
			jsonExtensions = JSON.stringify(e.extensions);
			jsonArgs = JSON.stringify(args);
		}catch(e){}
		try{
			stack = e.stack.substring(0,1023);
			jsonArgs = jsonArgs.stack.substring(0,1023);
		}catch(e){}
		try{
			await murmurAPI.query("insert into serverErrors (hostname,eventId,authToken,method,args,username,name,message,extensions,stack) values(?,?,?,?,?,?,?,?,?,?)",[process.env.HOSTNAME_FOR_UPLOADS, upload.id, "", "", jsonArgs, username, e.name, e.message, jsonExtensions, stack]);
		}catch(e){}

	}
}


(async() => {


	await subscribeRedisClient.connect();

	await subscribeRedisClient.subscribe('redis_UPLOAD_FINISHED', async(message, channel) => {
		var obj = null;
		try {
			obj = JSON.parse(message);
			console.log(obj);
		}catch (e){
			console.log(e);
		}
		await attemptProcessing();
	});

	console.log("start");
	while (1){
		await attemptProcessing();
		await delay(30000); 
	}
})();

async function fireFileCompletedRequest({bucketName,fileKey,size,previewImageFileKey,generatedPreviewUrl,duration, width, height}){
	console.log("fileUploadCompletedRequest");
	console.log([bucketName,fileKey,size,generatedPreviewUrl,duration].join("\t"));
	try {
		var result = await client.mutate({
			mutation: gql`mutation fileUploadCompleted($bucket: String!, $key: String!, $size: BigInt!, $previewImageFileKey: String, $generatedPreviewUrl: String, $duration: Int, $width: Int, $height: Int) {
				fileUploadCompleted(bucket:$bucket, key:$key, size:$size, previewImageFileKey: $previewImageFileKey, generatedPreviewUrl: $generatedPreviewUrl, duration: $duration, width: $width, height: $height) {
					success
				}
			}`,
			variables: {
				bucket: bucketName,
				key: fileKey,
				size: size,
				previewImageFileKey: previewImageFileKey,
				generatedPreviewUrl: generatedPreviewUrl,
				duration: duration,
				width: width,
				height: height
			},
			context: {
				headers: {
					authorization: process.env.FILE_UPLOAD_AUTH_TOKEN
				}
			}
		})
	}catch (e){
		console.log(e);
	}
}

async function delay(ms) {
	// return await for better async stack trace support in case of errors.
	return await new Promise(resolve => setTimeout(resolve, ms));
}
