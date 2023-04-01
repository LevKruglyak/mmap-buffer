# mmap-buffer
(Mostly) safe wrapper for a fixed-size file-backed buffer.

# Example

```rust
use mmap_buffer::BackedBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    {
        let mut buf = BackedBuffer::<i32>::new(100, "test.data")?;

        // These changes will be reflected in the file
        buf[10] = -10;
        buf[20] = 27;
    }
    
    // Later, we can load the same array
    let mut buf = BackedBuffer::<i32>::load("test.data")?;

    assert_eq!(buf[10], -10);
    assert_eq!(buf[20], 27);

    Ok(())
}
```
