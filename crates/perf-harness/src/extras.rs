//! Plugins the Debezium worker image does not ship, fetched from their
//! official distributions and verified against pinned SHA-256 digests:
//!
//! - Confluent's Avro converter (Confluent Hub), so a connector writes Avro
//!   through the same Confluent-compatible schema registry DeltaForge uses;
//! - GraalJS (Maven Central), the JavaScript engine of Debezium's scripting
//!   transformations (the image ships the transformations, not an engine);
//! - Confluent's S3 sink connector (Confluent Hub), which writes a topic to
//!   S3 as Parquet: Debezium's path to Parquet goes through Kafka.
//!
//! Files are cached under `<cache>/`; a cached file is re-verified on use.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Confluent's Avro converter plugin (Confluent Hub zip, all dependencies).
const AVRO_CONVERTER: (&str, &str) = (
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-avro-converter/versions/8.3.2/confluentinc-kafka-connect-avro-converter-8.3.2.zip",
    "f849f49ae500d1c02f94a50d1dd4b3e63d26433843007ac114a7f8e9fb3d6be8",
);

/// Confluent's S3 sink connector (Confluent Hub zip, all dependencies). The
/// hub publishes no digest for it: this is the digest of the archive as
/// first downloaded.
const S3_SINK: (&str, &str) = (
    "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/12.1.13/confluentinc-kafka-connect-s3-12.1.13.zip",
    "315c0ab43524d75a078cb33780adbad90cc6c8aa648fd38712e848d14efea485",
);

const GRAALJS_VERSION: &str = "25.4.4.1.1";

/// GraalJS and its runtime dependencies (`group:artifact`, SHA-256 of the
/// jar), as Maven resolves `org.graalvm.polyglot:polyglot`,
/// `org.graalvm.polyglot:js-community` and `org.graalvm.js:js-scriptengine`
/// at `GRAALJS_VERSION`.
const GRAALJS: [(&str, &str); 13] = [
    (
        "org.graalvm.polyglot:polyglot",
        "cbacc2628fd5f99dbcebff7d398bcddde43ea15ebadc873d1da4454c718727d8",
    ),
    (
        "org.graalvm.sdk:collections",
        "1b72cb9c9db533991bc308f2026a6fe986a4ca44b70b68fa6a2bb6c2fb9887ea",
    ),
    (
        "org.graalvm.sdk:nativeimage",
        "d633cb9895f01db443dbd2b6e54211cd505e679279f6745a84b1fee843156677",
    ),
    (
        "org.graalvm.sdk:word",
        "f20b7adf174a8c61018ce4e717d9dda65fec27d00540e547e63f1bafa400ac9a",
    ),
    (
        "org.graalvm.js:js-language",
        "50a91172497d9b94b83210af2e95472ba3bd39e6b5606c9e7b07f3de0022fa93",
    ),
    (
        "org.graalvm.regex:regex",
        "5b43366990bdd438c7dbd2ba0b3dba211acfaf67054275d0717bd0b8ea1d8dc9",
    ),
    (
        "org.graalvm.truffle:truffle-api",
        "9dcc81b6890d6fcc9ab57d17bc4430335f071d82b108c0af8556a28381978da6",
    ),
    (
        "org.graalvm.sdk:jniutils",
        "a9d7e6830e5068dcd6b780ccf2903c075da04b1b2f1fa6c8197b4e9b06d4c075",
    ),
    (
        "org.graalvm.shadowed:icu4j",
        "02f9e8a76ed2f8fa80d53054ca4632c7058354010df27c8f4e7e7443543f7e1e",
    ),
    (
        "org.graalvm.shadowed:xz",
        "556c0caae5b31ff84120655baeeb1d06677bc762a5632b36e7bb7b38a1773f7f",
    ),
    (
        "org.graalvm.truffle:truffle-runtime",
        "25818863ef8e50fc6329a876a033055c7418f20bedb051cfbb31c3d895fad7c9",
    ),
    (
        "org.graalvm.truffle:truffle-compiler",
        "06e9dcc0ed60616a4e1785f18ad6836c82295af7ee22f0802c69d46d325aacb2",
    ),
    (
        "org.graalvm.js:js-scriptengine",
        "2b10596a302e341cf3717835eb53df88e68ce1deb92cb8307cea0bfac0c3f874",
    ),
];

/// The pinned artifacts, for an invocation's record.
pub fn pins() -> serde_json::Value {
    serde_json::json!({
        "avro_converter": {"url": AVRO_CONVERTER.0, "sha256": AVRO_CONVERTER.1},
        "s3_sink": {"url": S3_SINK.0, "sha256": S3_SINK.1},
        "graaljs": {
            "version": GRAALJS_VERSION,
            "jars": GRAALJS.iter().map(|(c, h)| serde_json::json!({"artifact": c, "sha256": h})).collect::<Vec<_>>(),
        },
    })
}

/// Host directories to mount into the worker.
#[derive(Clone)]
pub struct Extras {
    /// The Avro converter's jars (a Kafka Connect plugin directory).
    pub avro_converter: PathBuf,
    /// The GraalJS jars (added to the MySQL connector's plugin directory).
    pub graaljs: PathBuf,
    /// The S3 sink connector's jars (a Kafka Connect plugin directory).
    pub s3_sink: PathBuf,
}

fn sha256(path: &Path) -> Result<String> {
    let out = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .context("sha256sum")?;
    Ok(String::from_utf8(out.stdout)?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_string())
}

/// `path`, downloaded from `url` unless already cached, verified.
fn fetch(url: &str, path: &Path, digest: &str) -> Result<()> {
    if !path.exists() {
        let partial = path.with_extension("part");
        let ok = std::process::Command::new("curl")
            .args(["-sfL", "-o"])
            .arg(&partial)
            .arg(url)
            .status()
            .context("curl")?
            .success();
        if !ok {
            bail!("download failed: {url}");
        }
        std::fs::rename(&partial, path)?;
    }
    let got = sha256(path)?;
    if got != digest {
        std::fs::remove_file(path).ok();
        bail!("{url}: SHA-256 {got}, pinned {digest}");
    }
    Ok(())
}

/// Fetch (or reuse) and verify the extras under `cache`.
pub fn prepare(cache: &Path) -> Result<Extras> {
    std::fs::create_dir_all(cache)?;
    // Docker bind mounts need absolute paths.
    let cache = &std::fs::canonicalize(cache)?;
    let avro_converter = hub_plugin(cache, AVRO_CONVERTER, "avro-converter")?;
    let s3_sink = hub_plugin(cache, S3_SINK, "s3-sink")?;
    let graaljs = cache.join(format!("graaljs-{GRAALJS_VERSION}"));
    std::fs::create_dir_all(&graaljs)?;
    for (coord, digest) in GRAALJS {
        let (group, artifact) =
            coord.split_once(':').context("group:artifact")?;
        let file = format!("{artifact}-{GRAALJS_VERSION}.jar");
        let url = format!(
            "https://repo1.maven.org/maven2/{}/{artifact}/{GRAALJS_VERSION}/{file}",
            group.replace('.', "/")
        );
        fetch(&url, &graaljs.join(&file), digest)?;
    }
    Ok(Extras {
        avro_converter,
        graaljs,
        s3_sink,
    })
}

/// A Confluent Hub plugin: `(url, sha256)` fetched (or reused) under
/// `cache`, verified, unpacked, and its `lib` copied to a writable plugin
/// directory `<name>-plugin` (the worker's entrypoint links the scripting jar
/// into every plugin directory, and runs as its own user).
fn hub_plugin(
    cache: &Path,
    (url, digest): (&str, &str),
    name: &str,
) -> Result<PathBuf> {
    let file = url.rsplit('/').next().context("plugin file name")?;
    let zip = cache.join(file);
    fetch(url, &zip, digest)?;
    let unpacked = cache.join(name);
    if !unpacked.exists() {
        let partial = cache.join(format!("{name}.part"));
        std::fs::remove_dir_all(&partial).ok();
        let ok = std::process::Command::new("unzip")
            .args(["-q", "-o"])
            .arg(&zip)
            .arg("-d")
            .arg(&partial)
            .status()
            .context("unzip")?
            .success();
        if !ok {
            bail!("unzip {}", zip.display());
        }
        std::fs::rename(&partial, &unpacked)?;
    }
    let plugin = cache.join(format!("{name}-plugin"));
    std::fs::remove_dir_all(&plugin).ok();
    let ok = std::process::Command::new("cp")
        .arg("-r")
        .arg(unpacked.join(file.trim_end_matches(".zip")).join("lib"))
        .arg(&plugin)
        .status()
        .context("cp")?
        .success();
    if !ok {
        bail!("copy the {name} plugin");
    }
    std::process::Command::new("chmod")
        .args(["-R", "a+rwX"])
        .arg(&plugin)
        .status()
        .context("chmod")?;
    Ok(plugin)
}
