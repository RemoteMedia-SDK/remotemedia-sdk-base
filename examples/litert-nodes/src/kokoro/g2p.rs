//! LiteRT-LM's English grapheme-to-phoneme front end (C++, `cpp/`), over its
//! C shim. The lexicon is the container's `en-lexicon` section; the mapping
//! that holds it must outlive the converter.

use std::ffi::{c_char, c_int, c_void, CStr};
use std::sync::Arc;

extern "C" {
    fn rm_en_g2p_create(lexicon: *const c_char, len: usize, error: *mut *mut c_char) -> *mut c_void;
    fn rm_en_g2p_text_to_ipa(
        handle: *mut c_void,
        text: *const c_char,
        len: usize,
        british: c_int,
        error: *mut *mut c_char,
    ) -> *mut c_char;
    fn rm_en_g2p_free(text: *mut c_char);
    fn rm_en_g2p_destroy(handle: *mut c_void);
}

pub struct EnglishG2p {
    handle: *mut c_void,
    _lexicon: Arc<dyn Send + Sync>,
}

// The converter is immutable after construction (thread-safe per its docs).
unsafe impl Send for EnglishG2p {}
unsafe impl Sync for EnglishG2p {}

unsafe fn take(text: *mut c_char) -> String {
    let out = CStr::from_ptr(text).to_string_lossy().into_owned();
    rm_en_g2p_free(text);
    out
}

impl EnglishG2p {
    /// `lexicon` borrows from `owner`, which the converter keeps alive.
    pub fn new(lexicon: &[u8], owner: Arc<dyn Send + Sync>) -> Result<Self, String> {
        let mut error: *mut c_char = std::ptr::null_mut();
        let handle =
            unsafe { rm_en_g2p_create(lexicon.as_ptr() as *const c_char, lexicon.len(), &mut error) };
        if handle.is_null() {
            return Err(unsafe { take(error) });
        }
        Ok(Self {
            handle,
            _lexicon: owner,
        })
    }

    pub fn text_to_ipa(&self, text: &str, british: bool) -> Result<String, String> {
        let mut error: *mut c_char = std::ptr::null_mut();
        let out = unsafe {
            rm_en_g2p_text_to_ipa(
                self.handle,
                text.as_ptr() as *const c_char,
                text.len(),
                british as c_int,
                &mut error,
            )
        };
        if out.is_null() {
            Err(unsafe { take(error) })
        } else {
            Ok(unsafe { take(out) })
        }
    }
}

impl Drop for EnglishG2p {
    fn drop(&mut self) {
        unsafe { rm_en_g2p_destroy(self.handle) }
    }
}
