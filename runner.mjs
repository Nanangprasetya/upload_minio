import * as Minio from "minio";
import * as dotenv from "dotenv";
import path from "path";

dotenv.config();

//! format promp `node {SCRIPT} {PREFIX} {FILE_DIRECTORY} {DAY_OF_DELETE}`
//! E.g
//! node runner.mjs database db_backup/db_prod_1202204.zip 7

const prefix = process.argv[2];
const sourceFile = process.argv[3];
const lastDay = process.argv[4] ?? 7;
const bucket = process.env.BUCKET;
const destinationObject = prefix + "/" + path.basename(sourceFile);

if (!prefix || !sourceFile) {
	console.error("Usage: node runner.mjs <prefix> <sourceFile> <day_of_delete>");
	process.exit(1);
}

const minioClient = new Minio.Client({
	endPoint: process.env.SERVER_ENDPOINT,
	accessKey: process.env.ACCESS_KEY,
	secretKey: process.env.SECRET_KEY,
	useSSL: true,
});

var metaData = {
	"Content-Type": "application/octet-stream",
};

const exists = await minioClient.bucketExists(bucket);
if (exists) {
	//! UPLOAD OBJECT
	try {
		await minioClient.fPutObject(
			bucket,
			destinationObject,
			sourceFile,
			metaData
		);
		console.log(
			`⬆️ File ${destinationObject} uploaded as object ${destinationObject} in bucket ${bucket}`
		);
	} catch (error) {
		console.error(
			`❌ Error when upload object in basket ${bucket}. \n ERROR: ${error}`
		);
	}

	//! CHECK OBJECTS FROM BUCKET (PRIPARATION TO CHECK LAST DATE OF OBJECT)
	const dateNow = new Date();
	dateNow.setDate(dateNow.getDate() - lastDay);

	const objectsStream = minioClient.listObjectsV2(bucket, prefix, true);
	objectsStream.on("data", async function (obj) {
		const lastModified = new Date(obj.lastModified);

		if (lastModified < dateNow) {
			try {
				//! REMOVE OBJECT
				await minioClient.removeObject(bucket, obj.name);
				console.log(
					`⬇️ File ${obj.name} create date at ${obj.lastModified} has been deleted from bucket ${bucket}`
				);
			} catch (error) {
				console.error(
					`❌ Error uploading object in basket ${bucket}. \n ERROR: ${error}`
				);
			}
		}
	});

	objectsStream.on("error", function (error) {
		console.error("❌ Error listing objects:", error);
	});
} else {
	console.error(`🛑 Bucket ${bucket} not found`);
	process.exit(1);
}
