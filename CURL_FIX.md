# librdkafka curl/OIDC Build Fix

## Problem
When building librdkafka without curl support, the build fails with:
```
fatal error: curl/curl.h: No such file or directory
```

This is a known issue:
- https://github.com/confluentinc/librdkafka/issues/5135
- https://github.com/confluentinc/librdkafka/issues/5204

## Solution
This repository includes local patches to work around the issue until the upstream PRs are merged.

### Changes Made

1. **build.rs** - Added explicit OAUTHBEARER OIDC disabling when curl is disabled:
   - For configure-based builds: Added `--disable-oauthbearer-oidc` flag
   - For CMake-based builds: Added `-DWITH_OAUTHBEARER_OIDC=0` definition

2. **librdkafka/src/rdkafka_conf.c** - Fixed preprocessor directive:
   - Changed `#ifdef WITH_OAUTHBEARER_OIDC` to `#if WITH_OAUTHBEARER_OIDC`
   - This ensures the check tests the value (0 or 1) rather than just existence

### Applying the Patch

If you need to reapply the patch (e.g., after updating the librdkafka submodule):

```bash
cd librdkafka
git apply ../librdkafka-curl-fix.patch
```

### Reverting the Patch

```bash
cd librdkafka
git apply -R ../librdkafka-curl-fix.patch
```

### When to Remove This Patch

This patch can be removed once the upstream librdkafka repository fixes this issue. Monitor the GitHub issues linked above for updates.
