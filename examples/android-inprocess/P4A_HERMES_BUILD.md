# Building python-for-android with Hermes Agent

This directory contains scripts to build a python-for-android (p4a) distribution that includes Hermes Agent and all its dependencies, suitable for in-process execution via PyO3 on Android.

## Files

- `requirements-hermes.txt` - Pinned dependencies for Hermes Agent core + selected extras
- `build_p4a_hermes_simple.sh` - Main build script using p4a's built-in pip support
- `build_p4a_hermes.sh` - Alternative build script with more options

## Prerequisites

1. **python-for-android** installed:
   ```bash
   pip install python-for-android
   ```

2. **Android SDK and NDK** configured:
   ```bash
   export ANDROID_SDK_ROOT=/path/to/android/sdk
   export ANDROID_NDK_ROOT=/path/to/android/ndk
   export PATH="${ANDROID_SDK_ROOT}/platform-tools:${PATH}"
   ```

3. **Rust targets** for cross-compilation (if building from source):
   ```bash
   rustup target add aarch64-linux-android
   ```

## Quick Start

```bash
# 1. Build the python-for-android distribution with Hermes Agent
cd /home/acidhax/dev/personal/remotemedia/remotemedia-sdk-base/examples/android-inprocess
./build_p4a_hermes_simple.sh

# This creates a distro at:
# ~/.local/share/python-for-android/dists/remotemedia_hermes/

# 2. Build and deploy the Android app (uses the new distro by default)
./android_build_deploy_test.sh --device <YOUR_DEVICE_IP:PORT>
```

## What's Included

The `requirements-hermes.txt` includes:

### Core Dependencies (from hermes-agent pyproject.toml)
- `openai==2.24.0` - OpenAI API client
- `python-dotenv==1.2.2` - Environment variable loading
- `fire==0.7.1` - CLI framework
- `httpx[socks]==0.28.1` - HTTP client with SOCKS support
- `rich==14.3.3` - Terminal formatting
- `tenacity==9.1.4` - Retry logic
- `pyyaml==6.0.3` - YAML parsing
- `ruamel.yaml==0.18.17` - Round-trip YAML
- `requests==2.33.0` - HTTP library
- `jinja2==3.1.6` - Template engine
- `pydantic==2.13.4` - Data validation (with pydantic-core)
- `prompt_toolkit==3.0.52` - Interactive CLI
- `croniter==6.0.0` - Cron scheduling
- `Markdown==3.10.2` - Markdown to HTML
- `PyJWT[crypto]==2.12.1` - JWT handling
- `psutil==7.2.2` - Process utilities
- `pathspec==1.1.1` - Gitignore patterns
- `fastapi==0.133.1` - Web framework
- `uvicorn[standard]==0.41.0` - ASGI server
- `Pillow==12.2.0` - Image processing

### CLI & Core Extras
- `simple-term-menu==1.6.6` - Terminal menus
- `mcp==1.26.0` + `starlette==1.0.1` - Model Context Protocol
- `agent-client-protocol==0.9.0` - ACP integration

### Optional Services
- `google-api-python-client==2.194.0` + auth - Google Workspace
- `youtube-transcript-api==1.2.4` - YouTube transcripts

### Base
- `numpy==2.4.3` - Required for many scientific packages

## Build Time

The build typically takes **10-30 minutes** depending on:
- Network speed (downloading wheels from PyPI)
- CPU cores (parallel builds)
- Whether pydantic-core needs compilation (it has aarch64 wheels)

## Troubleshooting

### pydantic-core compilation fails
If pydantic-core fails to build, ensure you have:
- Rust toolchain with `aarch64-linux-android` target
- `openssl` development headers

### Missing wheels for aarch64
Some packages may not have aarch64-linux-android wheels. p4a will attempt to build from source. If a build fails, you may need to:
1. Check if there's a newer version with Android wheels
2. Create a p4a recipe for the package
3. Exclude the package if not critical

### Python version
The script uses Python 3.11 by default (from central config). To change the version, update `config/python-version.toml` and re-run the build scripts.

## Output Structure

After successful build:
```
~/.local/share/python-for-android/dists/remotemedia_hermes/
├── _python_bundle__arm64-v8a/
│   ├── _python_bundle/          # Python stdlib + packages
│   │   ├── lib/
│   │   ├── lib/python${PYTHON_MAJOR_MINOR}/
│   │   └── runscripts/
│   └── libs/arm64-v8a/
│       ├── ${LIBPYTHON_NAME}
│       └── *.so                 # Native extensions
└── templates/
```

## Using in Android Build

The `android_build_deploy_test.sh` script now defaults to using `remotemedia_hermes` distro. You can override with environment variables:

```bash
# Use custom distro
export PYTHON_BUNDLE_SRC=/path/to/custom/_python_bundle
export PYTHON_NATIVE_LIBS_SRC=/path/to/custom/libs/arm64-v8a
./android_build_deploy_test.sh --device 100.76.57.109:37845
```

## Testing Hermes Agent on Android

After deploying, you can test the Hermes Agent integration:

1. The test manifest `hermes-agent-test.json` uses `HermesAgentTestPlugin`
2. Run the app and check logcat for:
   - `HermesAgentTestPlugin imports OK` - Success!
   - `HermesAgentTestPlugin import failed` - Check missing dependencies

## Adding More Packages

To add more packages to the distro:

1. Add to `requirements-hermes.txt`
2. Re-run `./build_p4a_hermes_simple.sh` (it will rebuild incrementally)
3. Rebuild and deploy the Android app

Note: Adding packages with native extensions (like `torch`, `tensorflow`) significantly increases build complexity and APK size. Only add what's truly needed for your use case.