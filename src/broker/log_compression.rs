//! Module for compressing and decompressing log files using gzip.
//!
//! The [`LogCompressor`] struct provides methods for compressing and decompressing log files.
//!
//! It is useful for managing log file sizes and ensuring efficient storage.

use flate2::Compression;
use flate2::write::{GzDecoder, GzEncoder};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

/// A utility for compressing and decompressing log files.
///
/// It uses the gzip format to compress and decompress files.
///
/// It is useful for managing log file sizes and ensuring efficient storage.
pub struct LogCompressor;

impl LogCompressor {
    /// Compresses a file using gzip compression.
    ///
    /// Compresses the contents of the input file and writes the compressed data to the output file.
    ///
    /// # Arguments
    /// * `input_path` - The path to the input file.
    /// * `output_path` - The path to the output file.
    ///
    /// # Returns
    /// An `io::Result` indicating the success or failure of the operation.
    ///
    /// # Errors
    /// If the input file cannot be read,
    /// or the output file cannot be written to, an error is returned.
    pub fn compress_file<P: AsRef<Path>>(input_path: P, output_path: P) -> io::Result<()> {
        let input_file = File::open(input_path)?;
        let output_file = File::create(output_path)?;
        let mut encoder = GzEncoder::new(output_file, Compression::default());
        let mut buffer = Vec::new();
        input_file.take(1024 * 1024).read_to_end(&mut buffer)?;
        encoder.write_all(&buffer)?;
        encoder.finish()?;
        Ok(())
    }

    /// Decompresses a gzip-compressed file.
    ///
    /// Reads the compressed data from the input file
    /// and writes the decompressed data to the output file.
    ///
    /// # Arguments
    /// * `input_path` - The path to the input file.
    /// * `output_path` - The path to the output file.
    ///
    /// # Returns
    /// An `io::Result` indicating the success or failure of the operation.
    ///
    /// # Errors
    /// If the input file cannot be read,
    /// or the output file cannot be written to, an error is returned.
    pub fn decompress_file<P: AsRef<Path>>(input_path: P, output_path: P) -> io::Result<()> {
        let input_file = File::open(input_path)?;
        let output_file = File::create(output_path)?;
        let mut decoder = GzDecoder::new(output_file);
        let mut buffer = Vec::new();
        input_file.take(1024 * 1024).read_to_end(&mut buffer)?;
        decoder.write_all(&buffer)?;
        decoder.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, remove_file};
    use std::io::{Read, Write};
    use tempfile::tempdir;

    /// Tests for compressing and decompressing files.
    ///
    /// # Purpose
    /// The tests verify that the [`LogCompressor`] can compress and decompress files correctly.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create an input file with some content.
    /// 3. Compress the input file.
    /// 4. Decompress the compressed file.
    /// 5. Verify that the decompressed file has the same content as the input file.
    /// 6. Clean up the temporary directory.
    #[test]
    fn test_compress_and_decompress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.txt");
        let compressed_path = dir.path().join("output.gz");
        let decompressed_path = dir.path().join("decompressed.txt");

        {
            let mut file = File::create(&input_path).unwrap();
            writeln!(file, "Hello, compression!").unwrap();
        }

        // Normal Flow: compression
        LogCompressor::compress_file(&input_path, &compressed_path).unwrap();
        assert!(compressed_path.exists());

        // Normal Flow: Defrosting
        LogCompressor::decompress_file(&compressed_path, &decompressed_path).unwrap();
        assert!(decompressed_path.exists());

        // Checking the contents
        let mut content = String::default();
        let mut file = File::open(&decompressed_path).unwrap();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content.trim(), "Hello, compression!");

        let _ = remove_file(&input_path);
        let _ = remove_file(&compressed_path);
        let _ = remove_file(&decompressed_path);
    }

    /// Tests for compressing a file that does not exist.
    ///
    /// # Purpose
    /// The test verifies that the [`LogCompressor::compress_file`] method
    /// returns an error when the input file does not exist.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Attempt to compress a file that does not exist.
    /// 3. Verify that the method returns an error.
    #[test]
    fn test_compress_file_not_found() {
        let dir = tempdir().unwrap();
        let missing_input = dir.path().join("no_such_file.txt");
        let compressed = dir.path().join("compressed.gz");

        // Exceptional Flow: The input file does not exist.
        let result = LogCompressor::compress_file(&missing_input, &compressed);
        assert!(result.is_err());
    }

    /// Tests for decompressing a file that does not exist.
    ///
    /// # Purpose
    /// The test verifies that the [`LogCompressor::decompress_file`] method
    /// returns an error when the input file does not exist.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Attempt to decompress a file that does not exist.
    /// 3. Verify that the method returns an error.
    #[test]
    fn test_decompress_file_not_found() {
        let dir = tempdir().unwrap();
        let missing_input = dir.path().join("no_such_file.gz");
        let output = dir.path().join("output.txt");

        // Exceptional Flow: The input file does not exist.
        let result = LogCompressor::decompress_file(&missing_input, &output);
        assert!(result.is_err());
    }

    /// Tests for compressing and decompressing an empty file.
    ///
    /// # Purpose
    /// The test verifies that the [`LogCompressor`] can compress and decompress an empty file.
    ///
    /// # Steps
    /// 1. Create a temporary directory.
    /// 2. Create an empty file.
    /// 3. Compress the empty file.
    /// 4. Decompress the compressed file.
    /// 5. Verify that the decompressed file is also empty.
    /// 6. Clean up the temporary directory.
    #[test]
    fn test_compress_and_decompress_empty_file() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("empty.txt");
        let compressed = dir.path().join("empty.gz");
        let decompressed = dir.path().join("decompressed_empty.txt");

        // Edge case: Create a blank file
        File::create(&input_path).unwrap();

        LogCompressor::compress_file(&input_path, &compressed).unwrap();
        assert!(compressed.exists());

        LogCompressor::decompress_file(&compressed, &decompressed).unwrap();
        assert!(decompressed.exists());

        // Check the contents
        let mut buf = String::default();
        let mut file = File::open(&decompressed).unwrap();
        file.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "");

        let _ = remove_file(&input_path);
        let _ = remove_file(&compressed);
        let _ = remove_file(&decompressed);
    }
}
