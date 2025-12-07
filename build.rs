use std::{
    borrow::Borrow,
    env,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::{self, Command},
};

fn run_command_or_fail<P, S>(dir: &str, cmd: P, args: &[S])
where
    P: AsRef<Path>,
    S: Borrow<str> + AsRef<OsStr>,
{
    let cmd = cmd.as_ref();
    let cmd = if cmd.components().count() > 1 && cmd.is_relative() {
        // If `cmd` is a relative path (and not a bare command that should be
        // looked up in PATH), absolutize it relative to `dir`, as otherwise the
        // behavior of std::process::Command is undefined.
        // https://github.com/rust-lang/rust/issues/37868
        PathBuf::from(dir)
            .join(cmd)
            .canonicalize()
            .expect("canonicalization failed")
    } else {
        PathBuf::from(cmd)
    };
    eprintln!(
        "Running command: \"{} {}\" in dir: {}",
        cmd.display(),
        args.join(" "),
        dir
    );
    let ret = Command::new(cmd).current_dir(dir).args(args).status();
    match ret.map(|status| (status.success(), status.code())) {
        Ok((true, _)) => (),
        Ok((false, Some(c))) => panic!("Command failed with error code {}", c),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {}", e),
    }
}

#[derive(Debug)]
struct Parser;

fn parse_doxygen(comment: &str) -> String {
    let url_regex = regex::Regex::new(r"(http[s]?://[^\s)]+)").unwrap();
    let mut result = String::new();
    let mut inside_list = false;

    for line in comment.lines() {
        let trimmed = line.trim_start();

        if trimmed.is_empty() {
            result.push('\n');
            continue;
        }

        let line = url_regex
            .replace_all(trimmed, "<$1>")
            .replace(r"\c ", "`")
            .replace(r"\p ", "`")
            .replace(r"\ref ", "[")
            .replace(r"[all]", r"\[all\]")
            .replace(r"[producer]", r"\[producer\]")
            .replace(r"[:port]", r"\[:port\]")
            .replace(r"[i]", r"\[i\]");

        if trimmed.starts_with("@brief") {
            result.push_str(trimmed.trim_start_matches("@brief").trim_start());
            result.push('\n');
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("@param ") {
            if let Some((name, desc)) = rest.trim().split_once(' ') {
                result.push_str(&format!("- `{}`: {}\n", name, desc.trim()));
                inside_list = true;
                continue;
            }
        }

        if trimmed.starts_with("@return") || trimmed.starts_with("@returns") {
            let desc = trimmed
                .trim_start_matches("@return")
                .trim_start_matches("s")
                .trim_start();
            result.push_str(&format!("Returns {}\n", desc));
            continue;
        }

        if inside_list && line.starts_with("    ") {
            result.push_str(line.trim_start());
            result.push('\n');
            continue;
        }

        result.push_str(&line);
        result.push('\n');
        inside_list = false;
    }

    result
        .lines()
        .map(|l| {
            if l.trim().is_empty() {
                "".to_string()
            } else {
                format!(" {}", l.trim_end())
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

impl bindgen::callbacks::ParseCallbacks for Parser {
    fn add_derives(&self, info: &bindgen::callbacks::DeriveInfo<'_>) -> Vec<String> {
        let mut derive_traits = vec![];
        match info.name {
            "rd_kafka_resp_err_t" | "rd_kafka_conf_res_t" => {
                derive_traits.push("::num_enum::TryFromPrimitive".to_string());
            }
            _ => {}
        }

        derive_traits
    }
    fn process_comment(&self, comment: &str) -> Option<String> {
        Some(parse_doxygen(comment))
    }
}

fn copy_license() {
    let license_path = Path::new("librdkafka").join("LICENSE");
    if !license_path.exists() {
        fs::copy(license_path, Path::new("LICENSE-librdkafka"))
            .expect("librdkafka license to be copied");
    }
}

fn generate_bindings() {
    let header_path = Path::new("librdkafka").join("src").join("rdkafka.h");
    let bindings_path = Path::new("src").join("bindings.rs");

    let current_bindings =
        fs::read_to_string(bindings_path.as_path()).expect("failed to read bindings");
    let bindings = bindgen::builder()
        .header(header_path.to_string_lossy())
        .rustified_enum(".*")
        .allowlist_function("rd_kafka.*")
        .allowlist_type("rd_kafka.*")
        .allowlist_var("rd_kafka.*|RD_KAFKA_.*")
        .allowlist_recursively(false)
        .raw_line("use libc::{FILE, sockaddr};")
        .raw_line("#[cfg(unix)]")
        .raw_line("use libc::{addrinfo, mode_t};")
        .default_macro_constant_type(bindgen::MacroTypeVariation::Signed)
        .parse_callbacks(Box::new(Parser))
        .generate()
        .expect("bindings to be generated")
        .to_string();
    let bindings = regex::Regex::new(r"(?m)^(\s*)pub fn (rd_kafka_conf_set_open_cb)\(")
        .unwrap()
        .replace_all(&bindings, "${1}#[cfg(unix)]\n${1}pub fn ${2}(")
        .to_string();

    if bindings != current_bindings {
        fs::write(bindings_path.as_path(), &bindings).expect("Failed to write updated bindings");
        Command::new("cargo")
            .arg("+nightly")
            .arg("fmt")
            .output()
            .expect("fmt to execute");
    }
}

fn apply_librdkafka_patch() {
    // Skip patching on docs.rs since the filesystem is read-only
    if env::var("DOCS_RS").is_ok() {
        eprintln!("Building on docs.rs, skipping patch (not needed for documentation)");
        return;
    }

    let patch_file = Path::new("librdkafka-curl-fix.patch");
    if !patch_file.exists() {
        eprintln!("Warning: librdkafka-curl-fix.patch not found, skipping patch");
        return;
    }

    // Check if patch is already applied by checking the exact line that gets changed
    let rdkafka_conf = Path::new("librdkafka/src/rdkafka_conf.c");
    if let Ok(content) = fs::read_to_string(rdkafka_conf) {
        // Get lines around line 59 (the line being patched)
        let lines: Vec<&str> = content.lines().collect();
        if lines.len() > 59 {
            let line_59 = lines[58]; // 0-indexed, so line 59 is at index 58

            // Check if the line has already been patched
            if line_59.contains("#if WITH_OAUTHBEARER_OIDC") {
                eprintln!("Patch already applied (line 59 has '#if'), skipping");
                return;
            } else if line_59.contains("#ifdef WITH_OAUTHBEARER_OIDC") {
                eprintln!("Patch needs to be applied (line 59 has '#ifdef')");
            }
        }
    }

    eprintln!("Applying librdkafka curl fix patch");
    run_command_or_fail(
        "librdkafka",
        "patch",
        &["-N", "-p1", "-i", "../librdkafka-curl-fix.patch"],
    );
}

fn main() {
    if env::var("CARGO_FEATURE_DYNAMIC_LINKING").is_ok() {
        eprintln!("librdkafka will be linked dynamically");

        let librdkafka_version = match env!("CARGO_PKG_VERSION")
            .split('+')
            .collect::<Vec<_>>()
            .as_slice()
        {
            [_rdsys_version, librdkafka_version] => *librdkafka_version,
            _ => panic!("Version format is not valid"),
        };

        let pkg_probe = pkg_config::Config::new()
            .cargo_metadata(true)
            .atleast_version(librdkafka_version)
            .probe("rdkafka");

        match pkg_probe {
            Ok(library) => {
                eprintln!("librdkafka found on the system:");
                eprintln!("  Name: {:?}", library.libs);
                eprintln!("  Path: {:?}", library.link_paths);
                eprintln!("  Version: {}", library.version);
            }
            Err(err) => {
                eprintln!(
                    "librdkafka {} cannot be found on the system: {}",
                    librdkafka_version, err
                );
                eprintln!("Dynamic linking failed. Exiting.");
                process::exit(1);
            }
        }
    } else {
        // Ensure that we are in the right directory
        let rdkafkasys_root = Path::new("rdkafka-sys");
        if rdkafkasys_root.exists() {
            assert!(env::set_current_dir(rdkafkasys_root).is_ok());
        }
        if !Path::new("librdkafka/LICENSE").exists() {
            eprintln!("Setting up submodules");
            run_command_or_fail(".", "git", &["submodule", "update", "--init"]);
        }

        apply_librdkafka_patch();

        eprintln!("Building and linking librdkafka statically");
        build_librdkafka();
    }

    if let Err(env::VarError::NotPresent) = env::var("DOCS_RS") {
        generate_bindings();
        copy_license();
    }
}

#[cfg(not(feature = "cmake-build"))]
fn build_librdkafka() {
    let mut configure_flags: Vec<String> = Vec::new();

    let mut cflags = Vec::new();
    if let Ok(var) = env::var("CFLAGS") {
        cflags.push(var);
    }

    let mut ldflags = Vec::new();
    if let Ok(var) = env::var("LDFLAGS") {
        ldflags.push(var);
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        configure_flags.push("--enable-ssl".into());
        if let Ok(openssl_root) = env::var("DEP_OPENSSL_ROOT") {
            cflags.push(format!("-I{}/include", openssl_root));
            ldflags.push(format!("-L{}/lib", openssl_root));
        }
    } else {
        configure_flags.push("--disable-ssl".into());
    }

    if env::var("CARGO_FEATURE_GSSAPI").is_ok() {
        configure_flags.push("--enable-gssapi".into());
        if let Ok(sasl2_root) = env::var("DEP_SASL2_ROOT") {
            cflags.push(format!("-I{}/include", sasl2_root));
            ldflags.push(format!("-L{}/build", sasl2_root));
        }
    } else {
        configure_flags.push("--disable-gssapi".into());
    }

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        // There is no --enable-zlib option, but it is enabled by default.
        if let Ok(z_root) = env::var("DEP_Z_ROOT") {
            cflags.push(format!("-I{}/include", z_root));
            ldflags.push(format!("-L{}/build", z_root));
        }
    } else {
        configure_flags.push("--disable-zlib".into());
    }

    if env::var("CARGO_FEATURE_CURL").is_ok() {
        // There is no --enable-curl option, but it is enabled by default.
        if let Ok(curl_root) = env::var("DEP_CURL_ROOT") {
            cflags.push("-DCURLSTATIC_LIB".to_string());
            cflags.push(format!("-I{}/include", curl_root));
        }
    } else {
        configure_flags.push("--disable-curl".into());
        // Explicitly disable OAUTHBEARER OIDC support when curl is disabled
        // to prevent build failures due to missing curl headers
        configure_flags.push("--disable-oauthbearer-oidc".into());
    }

    if env::var("CARGO_FEATURE_ZSTD").is_ok() {
        configure_flags.push("--enable-zstd".into());
        if let Ok(zstd_root) = env::var("DEP_ZSTD_ROOT") {
            cflags.push(format!("-I{}/include", zstd_root));
            ldflags.push(format!("-L{}", zstd_root));
        }
    } else {
        configure_flags.push("--disable-zstd".into());
    }

    if env::var("CARGO_FEATURE_EXTERNAL_LZ4").is_ok() {
        configure_flags.push("--enable-lz4-ext".into());
        if let Ok(lz4_root) = env::var("DEP_LZ4_ROOT") {
            cflags.push(format!("-I{}/include", lz4_root));
            ldflags.push(format!("-L{}", lz4_root));
        }
    } else {
        configure_flags.push("--disable-lz4-ext".into());
    }

    unsafe {
        env::set_var("CFLAGS", cflags.join(" "));
        env::set_var("LDFLAGS", ldflags.join(" "));
    }

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR missing");

    if !Path::new(&out_dir).join("LICENSE").exists() {
        // We're not allowed to build in-tree directly, as ~/.cargo/registry is
        // globally shared. mklove doesn't support out-of-tree builds [0], so we
        // work around the issue by creating a clone of librdkafka inside of
        // OUT_DIR, and build inside of *that* tree.
        //
        // https://github.com/edenhill/mklove/issues/17
        println!("Cloning librdkafka");
        run_command_or_fail(".", "cp", &["-a", "librdkafka/.", &out_dir]);
    }

    println!("Configuring librdkafka");
    run_command_or_fail(&out_dir, "./configure", configure_flags.as_slice());

    println!("Compiling librdkafka");
    if let Some(makeflags) = env::var_os("CARGO_MAKEFLAGS") {
        unsafe {
            env::set_var("MAKEFLAGS", makeflags);
        }
    }
    run_command_or_fail(
        &out_dir,
        if cfg!(target_os = "freebsd") {
            "gmake"
        } else {
            "make"
        },
        &["libs"],
    );

    println!("cargo:rustc-link-search=native={}/src", out_dir);
    println!("cargo:rustc-link-lib=static=rdkafka");
    println!("cargo:root={}", out_dir);
}

#[cfg(feature = "cmake-build")]
fn build_librdkafka() {
    let mut config = cmake::Config::new("librdkafka");
    let mut cmake_library_paths = vec![];

    config
        .define("RDKAFKA_BUILD_STATIC", "1")
        .define("RDKAFKA_BUILD_TESTS", "0")
        .define("RDKAFKA_BUILD_EXAMPLES", "0")
        // CMAKE_INSTALL_LIBDIR is inferred as "lib64" on some platforms, but we
        // want a stable location that we can add to the linker search path.
        // Since we're not actually installing to /usr or /usr/local, there's no
        // harm to always using "lib" here.
        .define("CMAKE_INSTALL_LIBDIR", "lib");

    if env::var("CARGO_FEATURE_LIBZ").is_ok() {
        config.define("WITH_ZLIB", "1");
        config.register_dep("z");
        if let Ok(z_root) = env::var("DEP_Z_ROOT") {
            cmake_library_paths.push(format!("{}/build", z_root));
        }
    } else {
        config.define("WITH_ZLIB", "0");
    }

    if env::var("CARGO_FEATURE_CURL").is_ok() {
        config.define("WITH_CURL", "1");
        config.register_dep("curl");
        if let Ok(curl_root) = env::var("DEP_CURL_ROOT") {
            config.define("CURL_STATICLIB", "1");
            cmake_library_paths.push(format!("{}/lib", curl_root));

            config.cflag("-DCURL_STATICLIB");
            config.cxxflag("-DCURL_STATICLIB");
            config.cflag(format!("-I{}/include", curl_root));
            config.cxxflag(format!("-I{}/include", curl_root));
            config.cflag(format!("-L{}/lib", curl_root));
            config.cxxflag(format!("-L{}/lib", curl_root));
            //FIXME: Upstream should be copying this in their build.rs
            fs::copy(
                format!("{}/build/libcurl.a", curl_root),
                format!("{}/lib/libcurl.a", curl_root),
            )
            .unwrap();
        }
    } else {
        config.define("WITH_CURL", "0");
        // Explicitly disable OAUTHBEARER OIDC support when curl is disabled
        // to prevent build failures due to missing curl headers
        config.define("WITH_OAUTHBEARER_OIDC", "0");
    }

    if env::var("CARGO_FEATURE_SSL").is_ok() {
        config.define("WITH_SSL", "1");
        config.define("WITH_SASL_SCRAM", "1");
        config.define("WITH_SASL_OAUTHBEARER", "1");
        config.register_dep("openssl");
    } else {
        config.define("WITH_SSL", "0");
    }

    if env::var("CARGO_FEATURE_GSSAPI").is_ok() {
        config.define("WITH_SASL", "1");
        config.register_dep("sasl2");
        if let Ok(sasl2_root) = env::var("DEP_SASL2_ROOT") {
            config.cflag(format!("-I{}/include", sasl2_root));
            config.cxxflag(format!("-I{}/include", sasl2_root));
        }
    } else {
        config.define("WITH_SASL", "0");
    }

    if env::var("CARGO_FEATURE_ZSTD").is_ok() {
        config.define("WITH_ZSTD", "1");
        config.register_dep("zstd");
    } else {
        config.define("WITH_ZSTD", "0");
    }

    if env::var("CARGO_FEATURE_EXTERNAL_LZ4").is_ok() {
        config.define("ENABLE_LZ4_EXT", "1");
        config.register_dep("lz4");
    } else {
        config.define("ENABLE_LZ4_EXT", "0");
    }

    if let Ok(system_name) = env::var("CMAKE_SYSTEM_NAME") {
        config.define("CMAKE_SYSTEM_NAME", system_name);
    }

    if let Ok(make_program) = env::var("CMAKE_MAKE_PROGRAM") {
        config.define("CMAKE_MAKE_PROGRAM", make_program);
    }

    if !cmake_library_paths.is_empty() {
        unsafe {
            env::set_var("CMAKE_LIBRARY_PATH", cmake_library_paths.join(";"));
        }
    }

    println!("Configuring and compiling librdkafka");
    let dst = config.build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-lib=static=rdkafka");
}
