//
// Subworkflow to preparse input data and generate samplesheet for nf-core/nanoseq
//

include { getParquet } from '../../../modules/local/get_parquet/main.nf'
include { setReferences } from '../../../modules/local/set_references/main.nf'
include { fileDir } from '../../../modules/local/file_dir/main.nf'
include { mergeFiles } from '../../../modules/local/merge_files/main.nf'
include { collectSampleInput } from '../../../modules/local/collect_sample_input/main.nf'

workflow prepare_samplesheet {
    take:
        ch_parquet_file
        parquetpath
        reference_id

    main:

        // Load and clean metadata from parquet
        parquet_file_ch = Channel.value(parquetpath)
        getParquet(parquet_file_ch)

        // Set reference fasta and gtf paths
        setReferences(reference_id)
        fasta_ch = setReferences.out.fasta
        gtf_ch   = setReferences.out.gtf

        // Split cleaned CSV into rows
        samples_ch = getParquet.out.splitCsv(header: true)

        // Pair each sample row with parquet path
        paired_samples_ch = samples_ch.map { row -> tuple(row, parquetpath) }

        // Extract directory info (baseDir, barcode, flowcell, etc.)
        fileDir(paired_samples_ch)

        // Construct fastq directory paths (using fallback if flowcell is missing)
        fastq_dirs_ch = fileDir.out.map { barcode, baseDir, expFolder, flowcell ->
            // If flowcell is empty, fallback to wildcard
            def flowcellGlob = flowcell ? "*_${flowcell}_*" : "*"
            "${baseDir}/${expFolder}/${expFolder}/${flowcellGlob}/fastq_pass/barcode${barcode}"
        }

        // Stage files from fastq directories
        staged_fastqs_ch = fastq_dirs_ch.map { dir -> tuple(dir, file("${dir}/*")) }

        // Merge FASTQ files per barcode
        mergeFiles(staged_fastqs_ch)

        // Combine metadata and paths to construct samplesheet lines
        collectSampleInput(
            samples_ch,
            mergeFiles.out,
            fasta_ch,
            gtf_ch,
            Channel.value(params.publishDir)
        )

        // Build final samplesheet CSV
        Channel.value('group,replicate,barcode,input_file,fasta,gtf')
            .concat(collectSampleInput.out)
            .collectFile(
                name: "${params.publishDir}/samplesheet.csv",
                sort: false,
                newLine: true
            )
            .set { samplesheet_csv }
    emit:
        samplesheet_csv
}