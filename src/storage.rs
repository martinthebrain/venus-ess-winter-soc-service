//! Removable-storage discovery and hardened atomic file operations.

use crate::config::{RuntimeConfig, STATE_MAX_BYTES};
use rustix::fs::{AtFlags, FileType, Mode, OFlags};
use rustix::io::Errno;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

const DEFAULT_MOUNTS_PATH: &str = "/proc/mounts";
const DEFAULT_MOUNTINFO_PATH: &str = "/proc/self/mountinfo";
const MAX_MOUNTS_BYTES: u64 = 1_048_576;
const MOUNT_PREFIXES: [&str; 3] = ["/media/", "/run/media/", "/mnt/"];
const DEVICE_PREFIXES: [&str; 3] = ["/dev/sd", "/dev/mmcblk", "/dev/disk/"];
const TEMP_FILE_ATTEMPTS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
struct MountIdentity {
    mount_id: u64,
    device: String,
    filesystem_root: Vec<u8>,
    filesystem_type: String,
    source: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemovableMediumIdentity {
    mount_root: PathBuf,
    device: u64,
    inode: u64,
    owner: u32,
    filesystem_id: u64,
    mount: Option<MountIdentity>,
}

impl RemovableMediumIdentity {
    fn capture(path: &Path) -> std::io::Result<Self> {
        let canonical = fs::canonicalize(path)?;
        if canonical != path {
            return Err(invalid_data(format!(
                "removable root is not a canonical path: {}",
                path.display()
            )));
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid_data(format!(
                "removable root is not a real directory: {}",
                path.display()
            )));
        }
        let mount = current_mount_identity(path);
        let simulated_test_medium = cfg!(test) && !is_removable_mount_path(path);
        if mount.is_none() && !simulated_test_medium {
            return Err(stale_medium(format!(
                "configured removable root is not mounted: {}",
                path.display()
            )));
        }
        let filesystem_id = rustix::fs::statvfs(path)
            .map_err(std::io::Error::from)?
            .f_fsid;
        Ok(Self {
            mount_root: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            filesystem_id,
            mount,
        })
    }

    fn open_verified_root(&self) -> std::io::Result<OwnedFd> {
        let root = rustix::fs::open(
            &self.mount_root,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        let stat = rustix::fs::fstat(&root).map_err(std::io::Error::from)?;
        let filesystem_id = rustix::fs::fstatvfs(&root)
            .map_err(std::io::Error::from)?
            .f_fsid;
        let current_mount = current_mount_identity(&self.mount_root);
        if stat.st_dev != self.device
            || stat.st_ino != self.inode
            || stat.st_uid != self.owner
            || filesystem_id != self.filesystem_id
            || current_mount != self.mount
        {
            return Err(stale_medium(format!(
                "removable medium changed before write: {}",
                self.mount_root.display()
            )));
        }
        Ok(root)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdLocation {
    pub path: PathBuf,
    pub description: String,
    pub identity: RemovableMediumIdentity,
}

#[must_use]
pub fn locate_sd(config: &RuntimeConfig) -> Option<SdLocation> {
    if let Some(path) = &config.sd_path {
        return sd_location(path, format!("SD path: {}", path.display()));
    }
    let roots = [
        Path::new("/media"),
        Path::new("/run/media"),
        Path::new("/mnt"),
    ];
    if let Some(label) = &config.sd_label {
        for root in roots {
            let path = root.join(label);
            if let Some(location) = sd_location(&path, format!("SD label: {}", path.display())) {
                return Some(location);
            }
        }
    }
    let mounts = read_mount_table(Path::new(DEFAULT_MOUNTS_PATH));
    mounted_storage_candidates(&mounts)
        .into_iter()
        .find_map(|path| sd_location(&path, format!("removable auto: {}", path.display())))
}

fn sd_location(path: &Path, description: String) -> Option<SdLocation> {
    RemovableMediumIdentity::capture(path)
        .ok()
        .map(|identity| SdLocation {
            path: path.to_path_buf(),
            description,
            identity,
        })
}

fn read_mount_table(path: &Path) -> String {
    let Ok(file) = File::open(path) else {
        return String::new();
    };
    let mut bytes = Vec::new();
    if file
        .take(MAX_MOUNTS_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() > usize::try_from(MAX_MOUNTS_BYTES).unwrap_or(usize::MAX)
    {
        return String::new();
    }
    String::from_utf8(bytes).unwrap_or_default()
}

#[must_use]
fn mounted_storage_candidates(text: &str) -> Vec<PathBuf> {
    let mut candidates = text
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let device = fields.next()?;
            let mount = fields.next()?.replace("\\040", " ");
            (DEVICE_PREFIXES
                .iter()
                .any(|prefix| device.starts_with(prefix))
                && MOUNT_PREFIXES
                    .iter()
                    .any(|prefix| mount.starts_with(prefix)))
            .then(|| PathBuf::from(mount))
        })
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

fn current_mount_identity(path: &Path) -> Option<MountIdentity> {
    let mountinfo = read_mount_table(Path::new(DEFAULT_MOUNTINFO_PATH));
    mount_identity_for(path, &mountinfo)
}

fn mount_identity_for(path: &Path, mountinfo: &str) -> Option<MountIdentity> {
    mountinfo
        .lines()
        .filter_map(parse_mountinfo_line)
        .filter(|(mount_point, _)| mount_point == path)
        .max_by_key(|(_, identity)| identity.mount_id)
        .map(|(_, identity)| identity)
}

fn parse_mountinfo_line(line: &str) -> Option<(PathBuf, MountIdentity)> {
    let (mount_fields, filesystem_fields) = line.split_once(" - ")?;
    let mut mount_fields = mount_fields.split_whitespace();
    let mount_id = mount_fields.next()?.parse::<u64>().ok()?;
    let _parent_id = mount_fields.next()?.parse::<u64>().ok()?;
    let device = mount_fields.next()?.to_owned();
    let filesystem_root = decode_mount_field(mount_fields.next()?)?;
    let mount_point = PathBuf::from(std::ffi::OsString::from_vec(decode_mount_field(
        mount_fields.next()?,
    )?));
    let mut filesystem_fields = filesystem_fields.split_whitespace();
    let filesystem_type = filesystem_fields.next()?.to_owned();
    let source = decode_mount_field(filesystem_fields.next()?)?;
    Some((
        mount_point,
        MountIdentity {
            mount_id,
            device,
            filesystem_root,
            filesystem_type,
            source,
        },
    ))
}

fn decode_mount_field(field: &str) -> Option<Vec<u8>> {
    let input = field.as_bytes();
    let mut output = Vec::with_capacity(input.len());
    let mut index = 0;
    while index < input.len() {
        if input[index] != b'\\' {
            output.push(input[index]);
            index += 1;
            continue;
        }
        let octal = input.get(index + 1..index + 4)?;
        if !octal.iter().all(u8::is_ascii_digit) || octal.iter().any(|byte| *byte > b'7') {
            return None;
        }
        let value = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + octal[2] - b'0';
        output.push(value);
        index += 4;
    }
    Some(output)
}

fn is_removable_mount_path(path: &Path) -> bool {
    MOUNT_PREFIXES.iter().any(|prefix| {
        let root = Path::new(prefix.trim_end_matches('/'));
        path != root && path.starts_with(root)
    })
}

/// Atomically replace a file on the exact removable medium captured earlier.
///
/// The destination is resolved relative to a freshly opened and verified
/// mount-root descriptor. If the mount disappeared or changed, no directory
/// or file below the path exposed by the underlying filesystem is touched.
///
/// # Errors
///
/// Returns an I/O error when the medium identity changed, the destination is
/// outside the captured root, or a hardened relative file operation fails.
pub(crate) fn atomic_write_removable(
    identity: &RemovableMediumIdentity,
    path: &Path,
    data: &[u8],
    fsync: bool,
) -> std::io::Result<()> {
    let relative = path.strip_prefix(&identity.mount_root).map_err(|_| {
        invalid_data(format!(
            "removable destination escapes mount root: {}",
            path.display()
        ))
    })?;
    validate_relative_destination(relative)?;
    let root = identity.open_verified_root()?;
    let (parent, file_name) = open_relative_parent(root, relative)?;
    atomic_write_at(&parent, &file_name, data, fsync)
}

fn validate_relative_destination(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid_data(format!(
            "invalid removable relative destination: {}",
            path.display()
        )));
    }
    Ok(())
}

fn open_relative_parent(
    root: OwnedFd,
    relative: &Path,
) -> std::io::Result<(OwnedFd, std::ffi::OsString)> {
    let file_name = relative
        .file_name()
        .ok_or_else(|| invalid_data("removable destination has no file name".to_owned()))?
        .to_os_string();
    let mut directory = root;
    if let Some(parent) = relative.parent() {
        for component in parent.components() {
            let Component::Normal(name) = component else {
                return Err(invalid_data("invalid removable parent path".to_owned()));
            };
            directory = open_or_create_directory_at(&directory, name)?;
        }
    }
    Ok((directory, file_name))
}

fn open_or_create_directory_at(
    parent: &OwnedFd,
    name: &std::ffi::OsStr,
) -> std::io::Result<OwnedFd> {
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW;
    let directory = match rustix::fs::openat(parent, name, flags, Mode::empty()) {
        Ok(directory) => directory,
        Err(Errno::NOENT) => {
            if let Err(error) = rustix::fs::mkdirat(parent, name, Mode::RWXU) {
                if error != Errno::EXIST {
                    return Err(error.into());
                }
            }
            rustix::fs::openat(parent, name, flags, Mode::empty()).map_err(std::io::Error::from)?
        }
        Err(error) => return Err(error.into()),
    };
    validate_directory_fd(&directory)?;
    Ok(directory)
}

fn validate_directory_fd(directory: &OwnedFd) -> std::io::Result<()> {
    let stat = rustix::fs::fstat(directory).map_err(std::io::Error::from)?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
        return Err(invalid_data(
            "removable path component is not a directory".to_owned(),
        ));
    }
    require_current_uid(stat.st_uid, "removable directory")
}

fn atomic_write_at(
    parent: &OwnedFd,
    file_name: &std::ffi::OsStr,
    data: &[u8],
    fsync: bool,
) -> std::io::Result<()> {
    validate_existing_target_at(parent, file_name)?;
    for _ in 0..TEMP_FILE_ATTEMPTS {
        let temporary_name = random_temporary_name(file_name)?;
        let descriptor = match rustix::fs::openat(
            parent,
            &temporary_name,
            OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(descriptor) => descriptor,
            Err(Errno::EXIST) => continue,
            Err(error) => return Err(error.into()),
        };
        let mut file = File::from(descriptor);
        let result = (|| {
            validate_open_regular(Path::new(file_name), &file)?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            file.write_all(data)?;
            file.flush()?;
            if fsync {
                file.sync_all()?;
            }
            drop(file);
            rustix::fs::renameat(parent, &temporary_name, parent, file_name)
                .map_err(std::io::Error::from)?;
            if fsync {
                rustix::fs::fsync(parent).map_err(std::io::Error::from)?;
            }
            Ok(())
        })();
        if result.is_err() {
            let _ = rustix::fs::unlinkat(parent, &temporary_name, AtFlags::empty());
        }
        return result;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique removable temporary file",
    ))
}

fn validate_existing_target_at(
    parent: &OwnedFd,
    file_name: &std::ffi::OsStr,
) -> std::io::Result<()> {
    let stat = match rustix::fs::statat(parent, file_name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => stat,
        Err(Errno::NOENT) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
        return Err(invalid_data(
            "refusing non-regular removable destination".to_owned(),
        ));
    }
    require_current_uid(stat.st_uid, "removable destination")
}

fn random_temporary_name(file_name: &std::ffi::OsStr) -> std::io::Result<std::ffi::OsString> {
    let printable_name = file_name.to_string_lossy();
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| std::io::Error::other(format!("secure random source failed: {error}")))?;
    let token = u128::from_ne_bytes(random);
    Ok(std::ffi::OsString::from(format!(
        ".{printable_name}.tmp-{token:032x}"
    )))
}

fn require_current_uid(actual: u32, label: &str) -> std::io::Result<()> {
    let expected = fs::metadata("/proc/self")?.uid();
    if actual != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("{label} is owned by another user"),
        ));
    }
    Ok(())
}

/// Create and verify a process-private runtime directory.
///
/// # Errors
///
/// Returns an I/O error when the path is a symlink, is not a directory, is
/// owned by another user, or cannot be restricted to mode `0700`.
pub fn prepare_private_directory(path: &Path) -> std::io::Result<()> {
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "runtime path is not a real directory: {}",
            path.display()
        )));
    }
    require_current_owner(path, &metadata)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    let mode = fs::symlink_metadata(path)?.permissions().mode() & 0o777;
    if mode != 0o700 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("runtime directory is not mode 0700: {}", path.display()),
        ));
    }
    Ok(())
}

/// Atomically replace a regular owner-controlled file.
///
/// The temporary file is created in the destination directory with a random
/// name, `O_NOFOLLOW`, `create_new(true)`, and mode `0600` before being renamed.
///
/// # Errors
///
/// Returns an I/O error when validation, creation, writing, syncing, or
/// renaming fails.
pub fn atomic_write(path: &Path, data: &[u8], fsync: bool) -> std::io::Result<()> {
    validate_parent_directory(path)?;
    validate_existing_target(path)?;
    let (temporary, mut file) = create_random_temporary(path)?;
    let result = (|| {
        file.write_all(data)?;
        file.flush()?;
        if fsync {
            file.sync_all()?;
        }
        drop(file);
        fs::rename(&temporary, path)?;
        if fsync {
            sync_parent_directory(path)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

/// Open an owner-controlled regular file for append without following links.
///
/// # Errors
///
/// Returns an I/O error when opening or validating the file fails.
pub(crate) fn open_secure_append(path: &Path) -> std::io::Result<File> {
    validate_parent_directory(path)?;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    validate_open_regular(path, &file)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

/// Open an owner-controlled regular lock file without following links.
///
/// # Errors
///
/// Returns an I/O error when opening or validating the file fails.
pub(crate) fn open_secure_lock(path: &Path) -> std::io::Result<File> {
    validate_parent_directory(path)?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)?;
    validate_open_regular(path, &file)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

/// Read a state file only when it is within the configured size limit.
///
/// # Errors
///
/// Returns an I/O error when opening, validating, or reading fails, or
/// invalid-data when the file exceeds the hard state-size bound.
pub fn read_bounded(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    read_secure_bounded(path, STATE_MAX_BYTES)
}

/// Read an owner-controlled regular file without following links.
///
/// # Errors
///
/// Returns an I/O error when validation or reading fails, or invalid-data when
/// the file exceeds `maximum_bytes`.
pub(crate) fn read_secure_bounded(
    path: &Path,
    maximum_bytes: u64,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    validate_parent_directory(path)?;
    validate_open_regular(path, &file)?;
    let size = file.metadata()?.len();
    if size > maximum_bytes {
        return Err(invalid_data(format!(
            "file exceeds {maximum_bytes} bytes: {}",
            path.display()
        )));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

fn create_random_temporary(path: &Path) -> std::io::Result<(PathBuf, File)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("state");
    for _ in 0..TEMP_FILE_ATTEMPTS {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random).map_err(|error| {
            std::io::Error::other(format!("secure random source failed: {error}"))
        })?;
        let token = u128::from_ne_bytes(random);
        let temporary = path.with_file_name(format!(".{file_name}.tmp-{token:032x}"));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temporary)
        {
            Ok(file) => {
                validate_open_regular(&temporary, &file)?;
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
                return Ok((temporary, file));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate a unique temporary file",
    ))
}

fn validate_existing_target(path: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(invalid_data(format!(
                    "refusing non-regular destination: {}",
                    path.display()
                )));
            }
            require_current_owner(path, &metadata)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn validate_parent_directory(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let metadata = fs::symlink_metadata(parent)?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid_data(format!(
            "parent is not a real directory: {}",
            parent.display()
        )));
    }
    require_current_owner(parent, &metadata)
}

fn validate_open_regular(path: &Path, file: &File) -> std::io::Result<()> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(invalid_data(format!(
            "refusing non-regular file: {}",
            path.display()
        )));
    }
    require_current_owner(path, &metadata)
}

fn require_current_owner(path: &Path, metadata: &fs::Metadata) -> std::io::Result<()> {
    let expected = fs::metadata("/proc/self")?.uid();
    if metadata.uid() != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("file is owned by another user: {}", path.display()),
        ));
    }
    Ok(())
}

fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(parent)?;
    let metadata = directory.metadata()?;
    if !metadata.file_type().is_dir() {
        return Err(invalid_data(format!(
            "parent is not a directory: {}",
            parent.display()
        )));
    }
    require_current_owner(parent, &metadata)?;
    directory.sync_all()
}

fn invalid_data(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn stale_medium(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, message)
}

#[cfg(test)]
mod tests {
    use super::{
        RemovableMediumIdentity, atomic_write, atomic_write_removable, mount_identity_for,
        mounted_storage_candidates, open_secure_append, prepare_private_directory, read_bounded,
        read_mount_table,
    };
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;

    #[test]
    fn atomic_write_replaces_complete_payload_with_private_permissions() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("state.json");
        atomic_write(&path, b"one", false).unwrap_or_else(|_| std::process::abort());
        atomic_write(&path, b"two", true).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            read_bounded(&path).ok().flatten().as_deref(),
            Some(&b"two"[..])
        );
        let mode = fs::metadata(&path)
            .map(|metadata| metadata.permissions().mode() & 0o777)
            .unwrap_or_default();
        assert_eq!(mode, 0o600);
        assert_eq!(fs::read_dir(dir.path()).ok().map(Iterator::count), Some(1));
    }

    #[test]
    fn atomic_write_and_append_refuse_symlink_targets() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let victim = dir.path().join("victim");
        let target = dir.path().join("state.json");
        fs::write(&victim, b"untouched").unwrap_or_else(|_| std::process::abort());
        symlink(&victim, &target).unwrap_or_else(|_| std::process::abort());

        assert!(atomic_write(&target, b"attack", false).is_err());
        assert!(open_secure_append(&target).is_err());
        assert_eq!(fs::read(&victim).ok().as_deref(), Some(&b"untouched"[..]));
    }

    #[test]
    fn predictable_legacy_temporary_symlink_is_never_used() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let victim = dir.path().join("victim");
        let target = dir.path().join("state.json");
        let legacy_temporary = dir
            .path()
            .join(format!(".state.json.tmp-{}", std::process::id()));
        fs::write(&victim, b"untouched").unwrap_or_else(|_| std::process::abort());
        symlink(&victim, legacy_temporary).unwrap_or_else(|_| std::process::abort());

        atomic_write(&target, b"new state", false).unwrap_or_else(|_| std::process::abort());

        assert_eq!(fs::read(&victim).ok().as_deref(), Some(&b"untouched"[..]));
        assert_eq!(fs::read(target).ok().as_deref(), Some(&b"new state"[..]));
    }

    #[test]
    fn symlinked_parent_directory_is_rejected() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let real_parent = dir.path().join("real");
        let linked_parent = dir.path().join("linked");
        fs::create_dir(&real_parent).unwrap_or_else(|_| std::process::abort());
        symlink(&real_parent, &linked_parent).unwrap_or_else(|_| std::process::abort());

        assert!(atomic_write(&linked_parent.join("state.json"), b"state", false).is_err());
        assert!(!real_parent.join("state.json").exists());
    }

    #[test]
    fn private_runtime_directory_is_mode_0700() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let runtime = root.path().join("runtime");
        prepare_private_directory(&runtime).unwrap_or_else(|_| std::process::abort());
        let mode = fs::metadata(runtime)
            .map(|metadata| metadata.permissions().mode() & 0o777)
            .unwrap_or_default();
        assert_eq!(mode, 0o700);
    }

    #[test]
    fn secure_append_creates_private_regular_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("log");
        let mut file = open_secure_append(&path).unwrap_or_else(|_| std::process::abort());
        file.write_all(b"entry")
            .unwrap_or_else(|_| std::process::abort());
        drop(file);
        assert_eq!(fs::read(&path).ok().as_deref(), Some(&b"entry"[..]));
        assert_eq!(
            fs::metadata(path)
                .map(|metadata| metadata.permissions().mode() & 0o777)
                .unwrap_or_default(),
            0o600
        );
    }

    #[test]
    fn removable_mount_detection_accepts_usb_sd_and_escaped_names() {
        let mounts = concat!(
            "/dev/root /media/root ext4 rw 0 0\n",
            "/dev/sdb1 /srv/not-removable ext4 rw 0 0\n",
            "/dev/sda1 /run/media/sda1 vfat rw 0 0\n",
            "/dev/mmcblk0p1 /media/Card\\040One ext4 rw 0 0\n",
            "/dev/disk/by-id/archive /mnt/archive ext4 rw 0 0\n",
        );
        assert_eq!(
            mounted_storage_candidates(mounts),
            vec![
                PathBuf::from("/media/Card One"),
                PathBuf::from("/mnt/archive"),
                PathBuf::from("/run/media/sda1"),
            ]
        );
    }

    #[test]
    fn mount_identity_binds_mount_device_root_type_and_source() {
        let mountinfo = concat!(
            "31 20 0:27 / /run rw,nosuid - tmpfs tmpfs rw\n",
            "42 31 8:17 /archive /run/media/Card\\040One rw,relatime - ext4 ",
            "/dev/disk/by-id/Card\\040One rw\n",
        );

        let identity =
            mount_identity_for(PathBuf::from("/run/media/Card One").as_path(), mountinfo)
                .unwrap_or_else(|| std::process::abort());

        assert_eq!(identity.mount_id, 42);
        assert_eq!(identity.device, "8:17");
        assert_eq!(identity.filesystem_root, b"/archive");
        assert_eq!(identity.filesystem_type, "ext4");
        assert_eq!(identity.source, b"/dev/disk/by-id/Card One");
    }

    #[test]
    fn removable_write_is_relative_to_the_verified_medium() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let medium = root.path().join("medium");
        fs::create_dir(&medium).unwrap_or_else(|_| std::process::abort());
        let identity =
            RemovableMediumIdentity::capture(&medium).unwrap_or_else(|_| std::process::abort());
        let target = medium.join("socSteuerung/ess_winter_logic.json");

        atomic_write_removable(&identity, &target, b"state", true)
            .unwrap_or_else(|_| std::process::abort());

        assert_eq!(fs::read(target).ok().as_deref(), Some(b"state".as_slice()));
    }

    #[test]
    fn stale_removable_request_never_creates_content_below_the_old_mount_path() {
        let root = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let medium = root.path().join("medium");
        fs::create_dir(&medium).unwrap_or_else(|_| std::process::abort());
        let identity =
            RemovableMediumIdentity::capture(&medium).unwrap_or_else(|_| std::process::abort());
        let replacement = root.path().join("replacement-medium");
        fs::create_dir(&replacement).unwrap_or_else(|_| std::process::abort());
        fs::remove_dir(&medium).unwrap_or_else(|_| std::process::abort());
        fs::rename(&replacement, &medium).unwrap_or_else(|_| std::process::abort());
        let target = medium.join("socSteuerung/ess_winter_logic.json");

        let error = atomic_write_removable(&identity, &target, b"state", true)
            .err()
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(!medium.join("socSteuerung").exists());
    }

    #[test]
    fn oversized_or_invalid_mount_tables_are_rejected() {
        let directory = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let oversized = directory.path().join("oversized");
        fs::write(&oversized, vec![b'x'; 1_048_577]).unwrap_or_else(|_| std::process::abort());
        assert!(read_mount_table(&oversized).is_empty());

        let invalid = directory.path().join("invalid");
        fs::write(&invalid, [0xff]).unwrap_or_else(|_| std::process::abort());
        assert!(read_mount_table(&invalid).is_empty());
    }
}
