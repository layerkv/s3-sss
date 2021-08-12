use anyhow::anyhow;
use async_trait::async_trait;
use base64::encode as b64encode;
use chrono::{DateTime, Utc};
use md5::{Digest, Md5};
use rusoto_core::{ByteStream, HttpClient, Region};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectOutput, GetObjectRequest, HeadObjectRequest, PutObjectRequest,
    S3Client, S3,
};
use sss::{SSSAccess, SSSErr, SSSFacade, SSSHead, SSSMeta};
use std::fs;
use std::path::Path;
use std::str::FromStr;
use tokio::fs::remove_file;
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream as tokio_io_stream;

struct AWSs3 {
    client: S3Client,
    bucket: String,
}

#[async_trait]
impl SSSFacade for AWSs3 {
    async fn init(access: &SSSAccess, region: &str, bucket: &str) -> Result<Self, SSSErr> {
        let p = StaticProvider::new(
            access.access_id.to_string(),
            access.access_secret.to_string(),
            None,
            None,
        );
        return match Region::from_str(region) {
            Ok(r) => Ok(Self {
                client: S3Client::new_with(HttpClient::new().unwrap(), p, r),
                bucket: bucket.to_string(),
            }),
            Err(e) => Err(SSSErr(e.to_string())),
        };
    }

    async fn upload(&self, key: &str, file: &Path, meta: Option<SSSMeta>) -> Result<bool, SSSErr> {
        return if let (Ok(f), Ok(i), Ok(c)) = (
            File::open(file).await,
            fs::metadata(file),
            md5sum_with_base64encode(file).await,
        ) {
            let mut meta = match meta {
                None => SSSMeta::new(),
                Some(m) => m,
            };
            meta.insert("md5checksum".to_string(), c.clone());
            let byte_stream = ByteStream::new(tokio_io_stream::new(f));
            let input = PutObjectRequest {
                bucket: self.bucket.to_string(),
                key: key.to_string(),
                body: Some(byte_stream),
                content_length: Some(i.len() as i64),
                content_md5: Some(c),
                metadata: Some(meta),
                ..Default::default()
            };
            match self.client.put_object(input).await {
                Ok(_) => Ok(true),
                Err(e) => Err(SSSErr(e.to_string())),
            }
        } else {
            Err(SSSErr(
                "the file does not exist or cannot be opened".to_string(),
            ))
        };
    }

    async fn delete(&self, key: &str) -> Result<bool, SSSErr> {
        let input = DeleteObjectRequest {
            bucket: self.bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };
        match self.client.delete_object(input).await {
            Ok(_) => Ok(true),
            Err(e) => Err(SSSErr(e.to_string())),
        }
    }

    async fn download(&self, key: &str, save_path: &Path) -> Result<bool, SSSErr> {
        let input = GetObjectRequest {
            bucket: self.bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };
        match self.client.get_object(input).await {
            Ok(o) => match check_and_save(o, save_path).await {
                Ok(_) => Ok(true),
                Err(e) => Err(SSSErr(e.to_string())),
            },
            Err(e) => Err(SSSErr(e.to_string())),
        }
    }

    async fn head(&self, key: &str) -> Result<SSSHead, SSSErr> {
        let input = HeadObjectRequest {
            bucket: self.bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        };
        match self.client.head_object(input).await {
            Ok(r) => match (r.last_modified, r.content_length, r.e_tag) {
                (Some(m_date), Some(c_len), Some(e_tag)) => match gmt2utc(m_date) {
                    Ok(last_modified) => Ok(SSSHead {
                        content_length: c_len,
                        e_tag: e_tag.trim_matches('"').to_string(),
                        last_modified,
                    }),
                    Err(e) => Err(SSSErr(e.to_string())),
                },
                _ => Err(SSSErr(
                    "the response of the HeadRequest is not expected".to_string(),
                )),
            },
            Err(e) => Err(SSSErr(e.to_string())),
        }
    }
}

// GMT, rfc2822; eg: Thu, 12 Aug 2021 04:54:09 GMT
fn gmt2utc(str_rfc2822: String) -> anyhow::Result<DateTime<Utc>> {
    let date = DateTime::parse_from_rfc2822(str_rfc2822.as_str())?;
    let date = DateTime::<Utc>::from(date);
    Ok(date)
}

// check that the value of metadata::md5checksum is equal to the checksum of the downloaded file,
// then save or remove the file.
async fn check_and_save(mut o: GetObjectOutput, save_path: &Path) -> anyhow::Result<bool> {
    if let Some(meta) = o.metadata {
        if let Some(md5checksum) = meta.get("md5checksum") {
            if let Some(body) = o.body.take() {
                let mut body = body.into_async_read();
                let mut file = File::create(save_path).await?;
                io::copy(&mut body, &mut file).await?;
                if md5checksum == &md5sum_with_base64encode(save_path).await? {
                    return Ok(true);
                }
                // check fail, then remove the file has been downloaded.
                remove_file(save_path).await?;
                return Err(anyhow!("checksum check failed, the file has been deleted"));
            }
            return Err(anyhow!("failed to get the body stream from ObjectOutput"));
        }
        return Err(anyhow!("metadata::md5checksum field is missing"));
    }
    return Err(anyhow!("metadata is missing"));
}

// compatible with `openssl md5 -binary PATH/TO/FILE | base64`
async fn md5sum_with_base64encode(file: &Path) -> anyhow::Result<String> {
    const BUFFER_SIZE: usize = 1024;
    let mut file = File::open(file).await?;
    let mut sh = Md5::default();
    let mut buffer = [0u8; BUFFER_SIZE];
    loop {
        let n = file.read(&mut buffer).await?;
        sh.update(&buffer[..n]);
        if n < BUFFER_SIZE {
            break;
        }
    }
    let s = b64encode(sh.finalize());
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::var;
    use tokio::runtime::Runtime;

    async fn init() -> Result<AWSs3, SSSErr> {
        let usage = r#"Error:
        the following environment variables required ————
        S3_ID       // access id,       eg: export S3_ID=ABCDEFGHIJKLMNOPQRST
        S3_SECRET   // access secret,   eg: export S3_SECRET=ABCDEF/ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFG
        S3_REGION   // region,          eg: export S3_REGION=REGIONNAME
        S3_BUCKET   // bucket name,     eg: export S3_BUCKET=BUCKETNAME
        S3_TEST_F   // test file path   eg: export S3_TEST_F=/PATH/TO/TESTFILE"#;
        match (
            var("S3_ID"),
            var("S3_SECRET"),
            var("S3_REGION"),
            var("S3_BUCKET"),
        ) {
            (Ok(k), Ok(s), Ok(r), Ok(b)) => {
                AWSs3::init(
                    &SSSAccess {
                        access_id: k.to_string(),
                        access_secret: s.to_string(),
                    },
                    &*r,
                    &*b,
                )
                .await
            }
            _ => Err(SSSErr(usage.to_string())),
        }
    }

    #[test]
    fn test_1stop() {
        let rt = Runtime::new().unwrap();
        let fp = Path::new(env!("S3_TEST_F"));
        let fi = fs::metadata(fp).unwrap();
        let fi_modified_time_original = fi.modified().unwrap();
        let mut fi_md5sum_original = String::new();
        rt.block_on(async {
            fi_md5sum_original = md5sum_with_base64encode(fp).await.unwrap();
        });

        test_upload();
        test_head();
        test_download();
        test_delete();

        rt.block_on(async {
            let fi = fs::metadata(fp).unwrap();
            let fi_modified_time_after = fi.modified().unwrap();
            assert!(
                fi_modified_time_after
                    .duration_since(fi_modified_time_original)
                    .unwrap()
                    .as_millis()
                    > 0
            );

            let fi_md5sum_after = md5sum_with_base64encode(fp).await.unwrap();
            assert_eq!(fi_md5sum_original, fi_md5sum_after);
        });
    }

    #[test]
    fn test_init() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            match init().await {
                Ok(r) => assert_eq!(r.bucket, env!("S3_BUCKET")),
                Err(e) => panic!("{}", e.to_string()),
            }
        });
    }

    fn test_upload() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            match init().await {
                Ok(c) => assert!(c
                    .upload("test_f", Path::new(env!("S3_TEST_F")), None,)
                    .await
                    .unwrap()),
                Err(e) => panic!("{}", e.to_string()),
            }
        });
    }

    fn test_download() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            match init().await {
                Ok(c) => assert!(c
                    .download("test_f", Path::new(env!("S3_TEST_F")))
                    .await
                    .unwrap()),
                Err(e) => panic!("{}", e.to_string()),
            }
        });
    }

    fn test_head() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            match init().await {
                Ok(c) => assert!(c.head("test_f").await.unwrap().content_length > 0),
                Err(e) => panic!("{}", e.to_string()),
            }
        });
    }

    fn test_delete() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            match init().await {
                Ok(c) => assert!(c.delete("test_f").await.unwrap()),
                Err(e) => panic!("{}", e.to_string()),
            }
        });
    }
}
