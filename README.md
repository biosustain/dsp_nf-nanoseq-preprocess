This is the repository for the nextflow pipeline to preprocess GridION fastq files and preparing a samplesheet.csv for nf-core/nanoseq DNA protocol.

The pipeline is will take an in-house parquet file with metadata (sample, replicate, barcode, and more) and together with a experiement project directory a compatible samplesheet.csv will be generated.

Expected input prarams:
`parquetpath` - path tp parquet file of metadata
`fastqsplit` - path to */fastq_pass/barcodeN/file_0.fastq.gz (until we are sure paths and relative paths to the parquet file are consistent, this will be used to generate paths to all relevant fastq files)
`fasta` - path to reference fasta file
`gtf` - path to reference fasta file
