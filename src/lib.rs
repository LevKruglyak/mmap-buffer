//! `mmap-backed` provides a (mostly) safe buffer which is backed
//! by a file using an `mmap` system call. These buffers don't support
//! dynamic reallocation, so they are fixed size. Here is an example:
//!
//! ```
//! use mmap_buffer::BackedBuffer;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     {
//!         let mut buf = BackedBuffer::<i32>::new(100, "test.data")?;
//!
//!         // These changes will be reflected in the file
//!         buf[10] = -10;
//!         buf[20] = 27;
//!     }
//!     
//!     // Later, we can load the same array
//!     let mut buf = BackedBuffer::<i32>::load("test.data")?;
//!
//!     assert_eq!(buf[10], -10);
//!     assert_eq!(buf[20], 27);
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
use std::{
    error::Error,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    path::Path,
};

use bytemuck::{try_cast_slice, try_cast_slice_mut, Pod};
use derive_more::{AsMut, AsRef};
use fs2::FileExt;
use memmap2::MmapOptions;

/// Helpful abstraction for some buffer, either backed by
/// a file, or stored in memory
pub enum Buffer<T: Pod> {
    /// Buffer backed by a file
    Disk(BackedBuffer<T>),
    /// In-memory buffer
    Memory(Vec<T>),
}

impl<T: Pod> Buffer<T> {
    /// Create a new buffer at the given path with a fixed capacity.
    /// This capacity is in units of `T`, not in bytes
    pub fn new_on_disk(capacity: usize, path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        Ok(Self::Disk(BackedBuffer::new(capacity, path)?))
    }

    /// Create a new buffer with fixed capacity in memory
    pub fn new_in_memory(capacity: usize) -> Self {
        Self::Memory(vec![T::zeroed(); capacity])
    }

    /// Load a buffer from an existing path.
    pub fn load_from_disk(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        Ok(Self::Disk(BackedBuffer::load(path)?))
    }

    /// Create an (in-memory) buffer from a vector
    pub fn from_vec_in_memory(data: Vec<T>) -> Self {
        Self::Memory(data)
    }

    /// Creates a new buffer at the given path and copies the contents of
    /// the slice to it. The created buffer will be the same size as the slice.
    pub fn from_slice_on_disk(data: &[T], path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        Ok(Self::Disk(BackedBuffer::copy_from_slice(data, path)?))
    }
}

/// A fixed size, mutable buffer of `T` backed by a file.
/// In order to avoid copying when reading and writing from such
/// a buffer, we require that `T: Pod`.
#[derive(AsRef, AsMut)]
pub struct BackedBuffer<T: Pod> {
    mmap: memmap2::MmapMut,
    file: Option<File>,
    _ph: PhantomData<T>,
}

impl<T: Pod> BackedBuffer<T> {
    /// Create a new buffer at the given path with a fixed capacity.
    /// This capacity is in units of `T`, not in bytes
    pub fn new(capacity: usize, path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;

        let capacity_bytes = capacity * std::mem::size_of::<T>();

        // Expand the file
        file.seek(SeekFrom::Start(0))?;
        file.allocate(capacity_bytes as u64)?;

        // Fill with zeroes (still unsure if there's a better way)
        const BLOCK_SIZE: usize = 4096;
        const BLOCK: [u8; BLOCK_SIZE] = [0; BLOCK_SIZE];

        // Convert size to bytes
        let mut size = capacity_bytes;
        while size > 0 {
            let block = usize::min(size, BLOCK_SIZE);
            file.write_all(&BLOCK[..block])?;
            size = size.checked_sub(block).unwrap();
        }

        unsafe { Self::from_file(file) }
    }

    /// Load a buffer from an existing path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // SAFETY: exclusive locks work internally when files read from path
        unsafe { Self::from_file(file) }
    }

    /// Creates a new buffer at the given path and copies the contents of
    /// the slice to it. The created buffer will be the same size as the slice.
    pub fn copy_from_slice(slice: &[T], path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let mut buf = Self::new(slice.len(), path)?;
        buf.copy_from_slice(slice);

        Ok(buf)
    }

    /// SAFETY: cannot `guarantee` advisory locks will work in this case, even
    /// within the same program (File clone does weird stuff)
    unsafe fn from_file(file: File) -> Result<Self, Box<dyn Error>> {
        // Establish advisory lock
        file.try_lock_exclusive()?;

        // Catch alignment issues ahead of time
        let mmap = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        let _: &[T] = try_cast_slice(&mmap[..])?;

        Ok(Self {
            mmap,
            file: Some(file),
            _ph: PhantomData,
        })
    }
}

impl<T: Pod> Deref for BackedBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: should predictably panic if file corrupted
        try_cast_slice(&self.mmap[..]).unwrap()
    }
}

impl<T: Pod> DerefMut for BackedBuffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: should predictably panic if file corrupted
        try_cast_slice_mut(&mut self.mmap[..]).unwrap()
    }
}

impl<T: Pod> Deref for Buffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Disk(backed_buffer) => backed_buffer.deref(),
            Self::Memory(vector) => vector.deref(),
        }
    }
}

impl<T: Pod> DerefMut for Buffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Disk(backed_buffer) => backed_buffer.deref_mut(),
            Self::Memory(vector) => vector.deref_mut(),
        }
    }
}

impl<T: Pod> AsRef<[T]> for Buffer<T> {
    fn as_ref(&self) -> &[T] {
        match self {
            Self::Disk(data) => data.deref(),
            Self::Memory(data) => data.deref(),
        }
    }
}

impl<T: Pod> AsMut<[T]> for Buffer<T> {
    fn as_mut(&mut self) -> &mut [T] {
        match self {
            Self::Disk(data) => data.deref_mut(),
            Self::Memory(data) => data.deref_mut(),
        }
    }
}

impl<T: Pod> Drop for BackedBuffer<T> {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            // Ignore the error, advisory locks are still kind of sus
            file.unlock().unwrap_or(());
        }
    }
}

#[cfg(test)]
impl<T: Pod> std::fmt::Debug for BackedBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?} of length {}", self.file, self.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::BackedBuffer;
    use std::{error::Error, fs::File, io::Write, path::Path};

    #[test]
    fn read() -> Result<(), Box<dyn Error>> {
        let tempdir = tempfile::tempdir().unwrap();
        let file_path = Path::join(tempdir.path(), "test");
        File::create(file_path.clone())
            .unwrap()
            .write("hello, world!".as_bytes())?;

        let mmap = BackedBuffer::<u8>::load(file_path).expect("");
        assert_eq!(&mmap[..], "hello, world!".as_bytes());

        Ok(())
    }

    #[test]
    fn write() -> Result<(), Box<dyn Error>> {
        let tempdir = tempfile::tempdir().unwrap();
        let file_path = Path::join(tempdir.path(), "test");
        File::create(file_path.clone())
            .unwrap()
            .write("hello, world!".as_bytes())?;

        let mut mmap = BackedBuffer::<u8>::load(file_path).expect("");
        mmap.copy_from_slice("halle, werld!".as_bytes());

        assert_eq!(&mmap[..], "halle, werld!".as_bytes());

        Ok(())
    }

    #[test]
    fn locking() -> Result<(), Box<dyn Error>> {
        let tempdir = tempfile::tempdir().unwrap();
        let file_path = Path::join(tempdir.path(), "test");
        File::create(file_path.clone()).unwrap();

        let _mmap_1 = BackedBuffer::<u8>::load(file_path.clone()).expect("");
        let _mmap_2 = BackedBuffer::<u8>::load(file_path.clone()).expect_err("");

        // Should be fine after unlocking
        drop(_mmap_1);
        let _mmap_2 = BackedBuffer::<u8>::load(file_path.clone()).expect("");

        Ok(())
    }
}
