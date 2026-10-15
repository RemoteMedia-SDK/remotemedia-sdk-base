//! Resident ARDY daemon ownership for pipeline executions on Unix hosts.
use crate::{manifest::Manifest, Error, Result};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    python: PathBuf,
    script: PathBuf,
    gpu: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    scene: Option<SceneConfig>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SceneConfig {
    profile: PathBuf,
    avatar: PathBuf,
    renderer_geometry: Option<PathBuf>,
}
struct Daemon {
    child: Child,
    directory: PathBuf,
    socket: PathBuf,
}
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
#[derive(Default)]
pub struct ManagedArdyDaemons(Mutex<BTreeMap<String, Daemon>>);
impl ManagedArdyDaemons {
    pub async fn prepare(&self, manifest: &Manifest) -> Result<Manifest> {
        let mut prepared = manifest.clone();
        let mut scene_bindings = Vec::new();
        for node in &mut prepared.nodes {
            let Some(raw) = node.params.get("managed_daemon") else {
                continue;
            };
            if node.node_type != "ArdyMotionNode"
                || node.params["mode"] != "office"
                || node.params.get("ardy_socket").is_some()
            {
                return Err(Error::Execution(
                    "managed_daemon requires office ArdyMotionNode without ardy_socket".into(),
                ));
            }
            let config: Config =
                serde_json::from_value(raw.clone()).map_err(|e| Error::Execution(e.to_string()))?;
            if config.scene.is_some() && !manifest.nodes.iter().any(|renderer| {
                renderer.node_type == "CcRenderNode"
                    && renderer.params.pointer("/avatar_session/motion_node").and_then(|v| v.as_str()) == Some(node.id.as_str())
            }) {
                return Err(Error::Execution("scene daemon requires a bound CcRenderNode".into()));
            }
            let key = raw.to_string();
            let mut daemons = self.0.lock().await;
            if let Some(daemon) = daemons.get_mut(&key) {
                if let Some(status) = daemon
                    .child
                    .try_wait()
                    .map_err(|e| Error::Execution(e.to_string()))?
                {
                    return Err(Error::Execution(format!("ARDY daemon exited: {status}")));
                }
            } else {
                daemons.insert(key.clone(), start(&config).await?);
            }
            node.params["ardy_socket"] = daemons[&key].socket.to_string_lossy().into_owned().into();
            if let Some(scene) = &config.scene {
                scene_bindings.push((node.id.clone(), daemons[&key].directory.join("scene.key"), scene.renderer_geometry.clone()));
            }
            node.params
                .as_object_mut()
                .unwrap()
                .remove("managed_daemon");
        }
        for (motion_node, key_file, geometry_file) in scene_bindings {
            let mut bound = false;
            for renderer in &mut prepared.nodes {
                if renderer.node_type == "CcRenderNode"
                    && renderer.params.pointer("/avatar_session/motion_node").and_then(|v| v.as_str()) == Some(motion_node.as_str()) {
                    renderer.params["scene_key_file"] = key_file.to_string_lossy().into_owned().into();
                    if let Some(path) = &geometry_file {
                        renderer.params["scene_geometry_file"] = path.to_string_lossy().into_owned().into();
                    }
                    bound = true;
                }
            }
            if !bound { return Err(Error::Execution("scene daemon requires a bound CcRenderNode".into())); }
        }
        Ok(prepared)
    }
}
#[cfg(unix)]
async fn start(config: &Config) -> Result<Daemon> {
    use std::os::unix::fs::DirBuilderExt;
    use tokio::io::{AsyncBufReadExt, BufReader};
    let directory = std::env::temp_dir().join(format!("remotemedia-ardy-{}", uuid::Uuid::new_v4()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|e| Error::Execution(e.to_string()))?;
    let socket = directory.join("ardy.sock");
    let mut command = Command::new(&config.python);
    command.arg("-u").arg(&config.script);
    if let Some(scene) = &config.scene {
        use std::{io::{Read, Write}, os::unix::fs::OpenOptionsExt};
        let mut key = [0_u8; 32];
        let provision = (|| -> std::io::Result<()> {
            std::fs::File::open("/dev/urandom")?.read_exact(&mut key)?;
            let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
                .open(directory.join("scene.key"))?;
            file.write_all(&key)
        })();
        if let Err(error) = provision {
            let _ = std::fs::remove_dir_all(&directory);
            return Err(Error::Execution(format!("scene key provisioning failed: {error}")));
        }
        command.arg("--scene-profile").arg(&scene.profile)
            .arg("--scene-avatar").arg(&scene.avatar)
            .arg("--scene-key-file").arg(directory.join("scene.key"));
    }
    let child = command
        .arg("--socket")
        .arg(&socket)
        .envs(&config.env)
        .env("CUDA_VISIBLE_DEVICES", &config.gpu)
        .env("ARDY_DEVICE", "cuda:0")
        .env_remove("ARDY_SOCKET")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| {
            let _ = std::fs::remove_dir_all(&directory);
            Error::Execution(format!("cannot start ARDY daemon: {e}"))
        })?;
    let mut daemon = Daemon {
        child,
        directory,
        socket,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        if let Some(status) = daemon
            .child
            .try_wait()
            .map_err(|e| Error::Execution(e.to_string()))?
        {
            return Err(Error::Execution(format!(
                "ARDY daemon failed during initialization: {status}"
            )));
        }
        if let Ok(stream) = tokio::net::UnixStream::connect(&daemon.socket).await {
            let mut line = String::new();
            tokio::time::timeout(
                Duration::from_secs(5),
                BufReader::new(stream).read_line(&mut line),
            )
            .await
            .map_err(|_| Error::Execution("ARDY readiness handshake timed out".into()))?
            .map_err(|e| Error::Execution(e.to_string()))?;
            let ready: serde_json::Value =
                serde_json::from_str(&line).map_err(|e| Error::Execution(e.to_string()))?;
            if ready["event"] != "ready" {
                return Err(Error::Execution(
                    "ARDY daemon returned invalid readiness".into(),
                ));
            }
            if config.scene.is_some() && (ready["scene_sampler"]["protocol"] != 3
                || ready["scene_sampler"]["authenticated_context"] != true) {
                return Err(Error::Execution("ARDY scene sampler did not register authenticated context".into()));
            }
            return Ok(daemon);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Execution("ARDY model startup timed out".into()));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
#[cfg(not(unix))]
async fn start(_config: &Config) -> Result<Daemon> {
    Err(Error::Execution(
        "managed ARDY daemon requires Unix domain sockets".into(),
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn scene_key_is_private_and_bound_only_to_its_renderer() {
        let fixture = tempfile::tempdir().unwrap();
        let script = fixture.path().join("scene_daemon.py");
        std::fs::write(&script, r#"
import os, socket, sys, stat
assert '--scene-profile' in sys.argv and '--scene-avatar' in sys.argv
key = sys.argv[sys.argv.index('--scene-key-file') + 1]
assert len(open(key, 'rb').read()) == 32
assert stat.S_IMODE(os.stat(key).st_mode) == 0o600
server = socket.socket(socket.AF_UNIX)
server.bind(sys.argv[sys.argv.index('--socket') + 1]); server.listen()
while True:
    client, _ = server.accept()
    client.sendall(b'{"event":"ready","scene_sampler":{"protocol":3,"authenticated_context":true}}\n')
    client.close()
"#).unwrap();
        let manifest: Manifest = serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{"name":"scene-fixture"}, "connections":[],
            "nodes":[{"id":"motion", "node_type":"ArdyMotionNode", "params":{
                "mode":"office", "managed_daemon":{"python":"/usr/bin/python3", "script":script,
                    "gpu":"0", "scene":{"profile":"profile.json", "avatar":"avatar.glb"}}}},
                {"id":"renderer", "node_type":"CcRenderNode", "params":{"avatar_session":{"motion_node":"motion"}}},
                {"id":"other", "node_type":"CcRenderNode", "params":{}}]
        })).unwrap();
        let owner = ManagedArdyDaemons::default();
        let prepared = owner.prepare(&manifest).await.unwrap();
        let key = PathBuf::from(prepared.nodes[1].params["scene_key_file"].as_str().unwrap());
        assert_eq!(std::fs::read(&key).unwrap().len(), 32);
        assert_eq!(std::fs::metadata(&key).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(prepared.nodes[2].params.get("scene_key_file").is_none());
        assert!(prepared.nodes[0].params.get("scene_key_file").is_none());
        drop(owner);
        assert!(!key.exists());
    }

    #[tokio::test]
    async fn readiness_reuse_failure_and_owner_cleanup() {
        let fixture = tempfile::tempdir().unwrap();
        let script = fixture.path().join("daemon.py");
        std::fs::write(
            &script,
            r#"
import os, socket, sys
assert os.environ['CUDA_VISIBLE_DEVICES'] == '0'
assert os.environ['ARDY_DEVICE'] == 'cuda:0'
assert os.environ['TEXT_ENCODER_DEVICE'] == 'cpu'
server = socket.socket(socket.AF_UNIX)
server.bind(sys.argv[sys.argv.index('--socket') + 1])
server.listen()
while True:
    client, _ = server.accept()
    client.sendall(b'{"event":"ready"}\n')
    client.close()
"#,
        )
        .unwrap();
        let manifest: Manifest = serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{"name":"fixture"}, "connections":[],
            "nodes":[{"id":"motion", "node_type":"ArdyMotionNode", "params":{
                "mode":"office", "managed_daemon":{
                    "python":"/usr/bin/python3", "script":script, "gpu":"0", "env":{"TEXT_ENCODER_DEVICE":"cpu"}
                }
            }}]
        }))
        .unwrap();
        let owner = ManagedArdyDaemons::default();
        let first = owner.prepare(&manifest).await.unwrap();
        let second = owner.prepare(&manifest).await.unwrap();
        assert_eq!(first.nodes[0].params, second.nodes[0].params);
        assert!(first.nodes[0].params.get("managed_daemon").is_none());
        let socket = PathBuf::from(first.nodes[0].params["ardy_socket"].as_str().unwrap());
        let directory = socket.parent().unwrap();
        assert_eq!(
            std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        {
            let mut daemons = owner.0.lock().await;
            daemons
                .values_mut()
                .next()
                .unwrap()
                .child
                .kill()
                .await
                .unwrap();
        }
        assert!(
            owner.prepare(&manifest).await.is_err(),
            "dead workers must not be silently replaced"
        );
        drop(owner);
        assert!(
            !directory.exists(),
            "owner must remove its private socket directory"
        );
    }
}
