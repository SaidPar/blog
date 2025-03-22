---
layout: post
title:  "Parallel Compression"
---
Zip file format is a first class citizen of IT infrastructure. The `.zip` file extension is as ubiquitous as `.pdf`, `.xml` & `.json`. The rise of this format authored by [Phil Katz](https://en.wikipedia.org/wiki/Phil_Katz) can be attributed to its open nature as shareware and the publishing of its [cross-platform, interoperable file storage and transfer format](https://pkwaredownloads.blob.core.windows.net/pkware-general/Documentation/APPNOTE-6.3.9.TXT). The specification allows any developer to create and extract `.zip` files. Check out [this great post](https://www.logikcull.com/blog/whats-in-a-zip-file-a-history-of-one-of-the-worlds-most-essential-file-types) to learn more about the history of the zip file format.

Compression is often single-threaded because the complex algorithms involved in data compression are not easily parallelizable. Compression algorithms often rely on analyzing data dependencies and making decisions based on previous data, making it difficult to split the work into independent chunks for parallel processing. 

Moreover, the zip file format doesn't necessarily lend itself well to parallelization. The anatomy of the zip file is essentially a collection of local files (entries) and a central directory at the end of the file. Each local file has some metadata associated with it in the entry header, followed by the central directory with additional metadata. 

![Zip File Decomposed](https://github.com/user-attachments/assets/e8574f4d-1e08-4d13-b671-bed2df29834f)

It is difficult for a zip file format writer to know the size of compressed bytes for each local file, particularly when those entries are large files or streams in which the entirety can not be held in memory. 

The problem of parallelization manifests itself at multiple levels: 
 1. Within the scope of a single local file, it is difficult to parallelize due to the nature of compression algorithms
 2. Within the scope of multiple local files, the nature of the zip file format suggests we must write the compressed data for each local file in a serial fashion.

Serial compression of multiple files is fairly straightforward in Java. We can use built in classes to create a `ZipOutputStream`, create one to many `ZipArchiveEntry` and write the bytes to `ZipOutputStream` which in turn compresses the bytes as it writes it to the zip file:

```java
try (ZipArchiveOutputStream zipOut = 
		new ZipArchiveOutputStream(Files.newOutputStream(zipFilePath))) {
  // Use a stream to walk through the file tree
  try (Stream<Path> paths = Files.walk(sourceDirPath)) {
	paths.filter(Files::isRegularFile).forEach(
		file -> {
			try {
				// Create the ZipArchiveEntry providing it a name.. In this case,
				// we preserve the name of the original file
				String archiveFilePath = sourceDirPath.relativize(file).toString()
				ZipArchiveEntry zipArchiveEntry = 
					new ZipArchiveEntry(archiveFilePath);

				// Set the archive entry context for the bytes we are about to write
				// to the ZipOutputStream
				zipOut.putArchiveEntry(zipArchiveEntry);

				// Copy the bytes to the ZipOutputStream.
				// Files.copy creates an input stream from the file
				// and performs a buffered transfer to the ZipOutputStream
				Files.copy(file, zipOut);
			} finally {
				// Close the archive entry so the next set of bytes is not
				// associated with this ZipArchiveEntry
				zipOut.closeArchiveEntry();
			}
		}
	);
  }
}
```

Against small datasets, this approach works just fine and is still reasonably performant. However, against large datasets you may find compression is the bottleneck in your application. 

Compression is a CPU bound task requiring complex pattern analysis to identify redundancies in the data. However, not all compression algorithms are equal in this respect. Some algorithms like [LZMA](https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Markov_chain_algorithm) utilize more processing power than older algorithms like zip's `DEFLATE` which has lower compression levels. Compression algorithms aren't easily parallelizable, and may only use a single thread for the compression itself. If your CPU has multiple cores, the CPU usage will appear low even if the core executing the compression is executing at 100%.

If your application does not exist in a resource constrained environment, you may be able to parallelize compression of local files prior to bundling them to the zip file. The Apache Commons library provides such a utility class, `ParallelScatterZipCreator`.  The `ScatterZipOutputStream`s can be compressed in parallel. When all streams have completed compression, they can be written as `ZipArchiveEntry`'s to the zip file.

```java
// Instantiate a ParallelScatterZipCreator
ParallelScatterZipCreator scatterZip = new ParallelScatterZipCreator();

// Setup the parallel compression tasks
try (Stream<Path> paths = Files.walk(sourceDirPath)) {
paths.filter(Files::isRegularFile).forEach(
	file -> {
		// Create the ZipArchiveEntry providing it a name and compression method
		String archiveFilePath = sourceDirPath.relativize(file).toString()
		ZipArchiveEntry zipArchiveEntry = new ZipArchiveEntry(archiveFilePath);
		entry.setMethod(ZipMethod.DEFLATED.getCode());

		// Provide the entry and supplier for parallel compression
		// See the file implentation for InputStreamSupplier below
		scatterZip.addArchiveEntry(entry, new FileInputStreamSupplier(sourceDirPath));
	}
}

// Setup the ZipArchiveOutputStream to bundle the compressed streams
try (ZipArchiveOutputStream zaos =
	new ZipArchiveOutputStream(Files.newOutputStream(Path.of(zipFilePath)))) {

	// writeTo waits for the completion of all parallel streams prior to 
	// writing the compressed bytes to the zip file 
    scatterZip.writeTo(zaos);
} catch (ExecutionException e) {
    throw new RuntimeException(e);
}
```

```java
static class FileInputStreamSupplier implements InputStreamSupplier {
    private Path sourceFile;

    FileInputStreamSupplier(Path sourceFile) {
        this.sourceFile = sourceFile;
    }

    @Override
    public InputStream get() {
        InputStream is = null;
        try {
            is = Files.newInputStream(sourceFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return is;
    }
}
```

The `ParallelScatterZipCreator` while a useful class has some inherent problems. Most notably `writeTo` is a blocking operation that waits on all parallel streams to complete compression. Internally, `ParallelScatterZipCreator` uses an `ExecutionService` to manage the threads.  Ideally, as streams complete the compression phase they would immediately be written to the zip file instead of waiting for their sibling streams to complete compression.  

`ParallelScatterZipCreator` also uses a backing store to write out the compressed data to a temporary file per stream. The reasoning is understandable as the amount of data in the input stream is unknown at the time of compression.  However, this results in potentially unnecessary disk IO for small streams. 

For a relatively small Input Stream, 
 1. The data is read into the stream 
 2. The data is compressed using a sliding window to search for redundancy patterns 
 3. The compressed bytes are written to a file
 4. The compressed bytes are then read from the file in order to write them again to the zip file. 
 
 This strategy while valid for large streams has the potential for a negative performance hit which may negate any gains from parallelization of compression.

I would love to see a JDK library class that builds on the implementation of `ParallelScatterZipCreator` in the following ways: 
 1. Instead of an `ExecutorService`, the class manages a `CompletionService` that optimistically writes to the zip as compression tasks complete. This of course alters the contract, as order is no longer preserved amongst local files in the zip. I question how important preserving order is. It seems a brittle design to transport a zip file in which the order of local files is important, but I'm sure there are valid use-cases out there.
 2. Writing a temporary file to disk to persist the compressed bytes should be the exception not the rule. For streams of relatively small size, under some configurable threshold should stay in memory and written to the zip file rather than saturate disk operations
 3. Parallelize compression of a stream when possible. `DEFLATED` is an old algorithm that uses a sliding window up to 32 kb in size to search for redundancy patterns for compression. This seems like a good candidate to chunk up the stream and parallelize the compression within a stream. Not all compression algorithms can take advantage of this approach, but there are a class of compression algorithms which may benefit from such an optimization.

I plan on a follow up detailing scenarios and performance benchmarks using the approaches outlined above. Your mileage may vary parallelizing compression depending on the bottlenecks of your data transmission pipelines. Profile your application to determine if you can take advantage of parallelization, and watch out for those side-effect performance hits lurking under the hood. 

If there are any assertions you don't agree with, I'd love to hear your thoughts. Please comment below.


