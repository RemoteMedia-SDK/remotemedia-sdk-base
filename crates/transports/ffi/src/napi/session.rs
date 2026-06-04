//! Session management for Node.js IPC
//!
//! Provides session-scoped channel isolation for multi-tenancy.

use super::channel::{make_channel_name, ChannelConfig, NapiChannel};
use super::error::IpcError;
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// Global session registry
static SESSIONS: std::sync::OnceLock<RwLock<HashMap<String, Arc<SessionInner>>>> =
    std::sync::OnceLock::new();

fn sessions() -> &'static RwLock<HashMap<String, Arc<SessionInner>>> {
    SESSIONS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Session configuration
#[napi(object)]
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Session identifier (8-64 alphanumeric chars + underscore/hyphen)
    pub id: String,
    /// Default channel configuration
    pub default_channel_config: Option<ChannelConfig>,
}

/// Internal session state
struct SessionInner {
    id: String,
    default_config: ChannelConfig,
    channels: RwLock<HashMap<String, Arc<NapiChannel>>>,
    is_active: AtomicBool,
}

/// IPC Session for channel namespace isolation
#[napi]
pub struct NapiSession {
    inner: Arc<SessionInner>,
}

#[napi]
impl NapiSession {
    /// Create a new session (use createSession() factory instead)
    fn new(config: SessionConfig) -> napi::Result<Self> {
        // Validate session ID
        validate_session_id(&config.id)?;

        let inner = Arc::new(SessionInner {
            id: config.id.clone(),
            default_config: config.default_channel_config.unwrap_or_default(),
            channels: RwLock::new(HashMap::new()),
            is_active: AtomicBool::new(true),
        });

        // Register in global sessions
        {
            let mut sessions = sessions().write().unwrap();
            if sessions.contains_key(&config.id) {
                return Err(IpcError::SessionError(format!(
                    "Session '{}' already exists",
                    config.id
                ))
                .into());
            }
            sessions.insert(config.id.clone(), inner.clone());
        }

        Ok(Self { inner })
    }

    /// Get the session ID
    #[napi(getter)]
    pub fn id(&self) -> String {
        self.inner.id.clone()
    }

    /// Check if the session is still active
    #[napi(getter)]
    pub fn is_active(&self) -> bool {
        self.inner.is_active.load(Ordering::SeqCst)
    }

    /// Create or open a channel within this session
    ///
    /// Channel names are automatically prefixed with session ID.
    #[napi]
    pub fn channel(
        &self,
        name: String,
        config: Option<ChannelConfig>,
    ) -> napi::Result<NapiChannel> {
        if !self.is_active() {
            return Err(IpcError::SessionError("Session is closed".to_string()).into());
        }

        // Validate channel name
        validate_channel_name(&name)?;

        let full_name = make_channel_name(&self.inner.id, &name);
        let channel_config = config.unwrap_or_else(|| self.inner.default_config.clone());

        // Check if channel already exists
        {
            let channels = self.inner.channels.read().unwrap();
            if let Some(existing) = channels.get(&name) {
                // Return a new handle sharing state with the existing channel
                return Ok(NapiChannel::with_shared_state(
                    full_name,
                    channel_config,
                    existing.get_is_open(),
                ));
            }
        }

        // Create new channel
        let channel = NapiChannel::new(full_name.clone(), channel_config.clone());
        let is_open = channel.get_is_open();

        // Store reference (sharing the same is_open state)
        {
            let mut channels = self.inner.channels.write().unwrap();
            channels.insert(
                name.clone(),
                Arc::new(NapiChannel::with_shared_state(
                    full_name,
                    channel_config,
                    is_open,
                )),
            );
        }

        Ok(channel)
    }

    /// List all channels in this session
    #[napi]
    pub fn list_channels(&self) -> Vec<String> {
        let channels = self.inner.channels.read().unwrap();
        channels.keys().cloned().collect()
    }

    /// Close the session and all its channels
    #[napi]
    pub fn close(&self) {
        self.inner.is_active.store(false, Ordering::SeqCst);

        // Close all channels
        {
            let channels = self.inner.channels.read().unwrap();
            for channel in channels.values() {
                channel.close();
            }
        }

        // Remove from global registry
        {
            let mut sessions = sessions().write().unwrap();
            sessions.remove(&self.inner.id);
        }
    }
}

impl Drop for NapiSession {
    fn drop(&mut self) {
        // Don't close on drop - session might have multiple references
        // Only close when explicitly called or all references are dropped
    }
}

/// Validate session ID format
fn validate_session_id(id: &str) -> napi::Result<()> {
    if id.len() < 8 || id.len() > 64 {
        return Err(
            IpcError::InvalidArgument("Session ID must be 8-64 characters".to_string()).into(),
        );
    }

    if !id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(IpcError::InvalidArgument(
            "Session ID must contain only alphanumeric characters, underscore, or hyphen"
                .to_string(),
        )
        .into());
    }

    if id.contains(':') || id.contains('/') {
        return Err(IpcError::InvalidArgument(
            "Session ID cannot contain ':' or '/' (reserved by iceoryx2)".to_string(),
        )
        .into());
    }

    Ok(())
}

/// Validate channel name format
fn validate_channel_name(name: &str) -> napi::Result<()> {
    if name.is_empty() || name.len() > 128 {
        return Err(
            IpcError::InvalidArgument("Channel name must be 1-128 characters".to_string()).into(),
        );
    }

    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(IpcError::InvalidArgument(
            "Channel name must contain only alphanumeric characters, underscore, or hyphen"
                .to_string(),
        )
        .into());
    }

    Ok(())
}

/// Create a new IPC session
#[napi]
pub fn create_session(config: SessionConfig) -> napi::Result<NapiSession> {
    NapiSession::new(config)
}

/// Get an existing session by ID
#[napi]
pub fn get_session(session_id: String) -> Option<NapiSession> {
    let sessions = sessions().read().unwrap();
    sessions.get(&session_id).map(|inner| NapiSession {
        inner: inner.clone(),
    })
}

/// List all active session IDs
#[napi]
pub fn list_sessions() -> Vec<String> {
    let sessions = sessions().read().unwrap();
    sessions.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_id_validation() {
        // Valid IDs
        assert!(validate_session_id("session_123").is_ok());
        assert!(validate_session_id("my-session-id").is_ok());
        assert!(validate_session_id("ABCD1234").is_ok());

        // Invalid IDs
        assert!(validate_session_id("short").is_err()); // Too short
        assert!(validate_session_id("has:colon").is_err()); // Reserved char
        assert!(validate_session_id("has/slash").is_err()); // Reserved char
        assert!(validate_session_id("").is_err()); // Empty
    }

    #[test]
    fn test_channel_name_validation() {
        // Valid names
        assert!(validate_channel_name("audio_input").is_ok());
        assert!(validate_channel_name("video-output").is_ok());

        // Invalid names
        assert!(validate_channel_name("").is_err()); // Empty
        assert!(validate_channel_name("has:colon").is_err());
    }
}
