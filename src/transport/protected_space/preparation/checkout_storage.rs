use std::fs::{self, OpenOptions};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

pub(super) fn check(path: &Path, limit: u64) -> io::Result<()> {
    let invalid = || io::Error::other("requires a private, dedicated, bounded checkout mount");
    if limit == 0 || !path.is_absolute() || fs::canonicalize(path)? != path {
        return Err(invalid());
    }
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let meta = root.metadata()?;
    if meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
        return Err(invalid());
    }
    let name = path.to_str().ok_or_else(invalid)?;
    // procfs escapes these four characters in mount table paths.
    let name = name
        .replace('\\', "\\134")
        .replace(' ', "\\040")
        .replace('\t', "\\011")
        .replace('\n', "\\012");
    let mount_id = fs::read_to_string(format!("/proc/self/fdinfo/{}", root.as_raw_fd()))?;
    let mount_id = mount_id
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .ok_or_else(invalid)?
        .trim()
        .as_bytes();
    let table = fs::read("/proc/self/mountinfo")?;
    if !dedicated_mount(&table, mount_id, name.as_bytes()) {
        return Err(invalid());
    }

    let mut stats = MaybeUninit::<libc::statvfs>::uninit();
    // fstatvfs fills stats on success and reads the same open mount as fdinfo.
    if unsafe { libc::fstatvfs(root.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stats = unsafe { stats.assume_init() };
    let required = libc::ST_NOSUID | libc::ST_NODEV | libc::ST_NOEXEC;
    let size = stats.f_blocks.checked_mul(stats.f_frsize);
    if stats.f_flag & required != required
        || stats.f_flag & libc::ST_RDONLY != 0
        || stats.f_files == 0
        || !matches!(size, Some(bytes) if bytes > 0 && bytes <= limit)
    {
        return Err(invalid());
    }
    Ok(())
}

fn dedicated_mount(table: &[u8], id: &[u8], path: &[u8]) -> bool {
    let records = || table.split(|&b| b == b'\n').filter(|line| !line.is_empty());
    let Some(selected) = records().find(|line| line.split(|&b| b == b' ').next() == Some(id))
    else {
        return false;
    };
    let mut fields = selected.split(|&b| b == b' ');
    let selected_device = fields.nth(2);
    if fields.next() != Some(b"/")
        || fields.next() != Some(path)
        || fields.find(|field| *field == b"-").is_none()
        || !matches!(fields.next(), Some(b"ext4" | b"tmpfs"))
    {
        return false;
    }
    records().filter(|line| *line != selected).all(|line| {
        let mut fields = line.split(|&b| b == b' ');
        let other_device = fields.nth(2);
        let Some(other_path) = fields.nth(1) else {
            return false;
        };
        // Aliases share the block budget with other users. Nested mounts can
        // move writes outside it. Stacked mounts hide the actual checkout.
        other_device.is_some()
            && other_device != selected_device
            && other_path != path
            && !other_path
                .strip_prefix(path)
                .is_some_and(|suffix| suffix.starts_with(b"/"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mount_must_be_a_whole_filesystem_without_aliases_or_submounts() {
        let volume = b"8 3 0:25 / /owned\\040checkouts rw - tmpfs tmpfs rw\n";
        let root = b"3 0 8:1 / / rw - ext4 /dev/root rw\n";
        let name = b"/owned\\040checkouts";
        assert!(dedicated_mount(
            &[root.as_slice(), volume].concat(),
            b"8",
            name
        ));
        assert!(!dedicated_mount(
            volume,
            b"8",
            b"/owned\\040checkouts/subdirectory"
        ));
        assert!(!dedicated_mount(volume, b"3", name));
        for additional in [
            b"9 3 0:25 / /alias rw - tmpfs tmpfs rw\n".as_slice(),
            b"9 8 8:1 /elsewhere /owned\\040checkouts/nested rw - ext4 /dev/root rw\n",
            b"9 3 8:1 /elsewhere /owned\\040checkouts rw - ext4 /dev/root rw\n",
        ] {
            assert!(!dedicated_mount(&[volume, additional].concat(), b"8", name));
        }
        let bind = b"8 3 0:25 /elsewhere /owned\\040checkouts rw - tmpfs tmpfs rw\n";
        assert!(!dedicated_mount(bind, b"8", name));
    }

    #[test]
    fn an_ordinary_directory_does_not_supply_a_checkout_budget() {
        let dir = tempfile::tempdir().unwrap();
        assert!(check(dir.path(), u64::MAX).is_err());
    }
}
