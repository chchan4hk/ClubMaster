/**
 * Downloads AWS RDS global CA bundle for sslrootcert (same source as:
 * curl -o global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
 */
import fs from "fs";
import https from "https";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const url =
  "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem";
const outDir = path.join(__dirname, "..", "certs");
const outFile = path.join(outDir, "global-bundle.pem");

function download() {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => resolve(Buffer.concat(chunks)));
      })
      .on("error", reject);
  });
}

await fs.promises.mkdir(outDir, { recursive: true });
const body = await download();
await fs.promises.writeFile(outFile, body, "utf8");
console.log("Wrote", outFile);
