#!/usr/bin/env nextflow

// flowcell position should not be considered, if two folders have same flow cell ID, 
// all folders should be vollected by the glob pattern.
// set default baseRefPath = "az://nextflowstorage/references/" ?
// decide a way for reference version number, right now, .1 is expected to be included in the ID.

// modules in subworkflow should be nf-core modules

// code is not working if flowcell is blank, for testing with currrent data comment line out at that part

/*
 * Benchling metadata extraction from 
 * parquet to csv and sanitise barcode input
 * by stripping letters from column 3 (sample_barcode)
 */

 // get flowcell ID from parquet file

process getParquet{
        container 'jbjespersen/parquet:test'
        input:
                path parquetpath
        output:
                path "sample_data_1.csv"
        script:
        """
        parquet-tools csv --columns group,replicate,sample_barcode,flow_cell_id,nucleic_acid_type nanopore_sequencing_submission_sample.parquet > sample_data.csv
        head -n 1 sample_data.csv > sample_data_1.csv
        awk -F ',' -v OFS=',' 'FNR == 1 {next} { sub("[a-zA-Z]+", "", \$3); print }' sample_data.csv >> sample_data_1.csv
        """
}

/*
* Set reference files for the pipeline
* by setting the fasta and gtf file paths
*/
process setReferences{
        container 'jbjespersen/parquet:test'
        input:
                val referenceID
        output:
                env (fasta), emit: fasta
                env (gtf), emit: gtf
        script:
        def baseRefPath = params.baseRefPath
        """
        fasta=${baseRefPath}/${referenceID}.fasta
        gtf=${baseRefPath}/${referenceID}.gtf
        """
}

/*
 * Extract the file paths for the fastq files
 * from the parquet file path
 * using regex
 */

// def makes local var, then not needed in bash.

process fileDir{
        container 'jbjespersen/parquet:test'
        input:
                tuple val(sample), val(parquetpath)
        output:
                tuple env(formattedBarcode), env(baseDir), env(expFolder), env(flowcell)
        script:
        def path = parquetpath
        def regex = /^(.+?)\/([A-Z0-9]+)\/benchling/
        def baseDir = (path =~ regex)[0][1]
        def expFolder = (path =~ regex)[0][2]
        def formattedBarcode = String.format("%02d", sample.sample_barcode as Integer)
        def flowcell = sample.flow_cell_id
                """
                formattedBarcode=${formattedBarcode}
                baseDir=${baseDir}
                expFolder=${expFolder}
                flowcell=${flowcell}
                """
}

/*
 * Merge the fastq files
 * from the same barcode
 * into one file
 * to get input files in consisten order they are inputted as input
 * so they get consistent order in unix
 */

// this one could be nf-core module, the rest are more internal usage

process mergeFiles{
        container 'jbjespersen/parquet:test'
        publishDir(
           path: "${params.publishDir}/fastq_merged",
           overwrite: true,
                mode: 'copy'
        )
        input:
                tuple val(dir), path(files, stageAs: 'inputs/*')
        output:
                path "merged_${dir.replaceAll('.*/', '')}.fastq.gz"
        script:
        """
        cat inputs/* > merged_${dir.replaceAll('.*/', '')}.fastq.gz
        """
}

/*
 * Collect the new file paths and metadata
 * to create individual lines for each sample to go into the samplesheet
 */

process collectSampleInput{
        container 'jbjespersen/parquet:test'
        input:
                val(sample) 
                path(mergedFile) 
                val(fasta) 
                val(gtf)
                val(publishDir)
        output:
                env (sampleLine)
        script:
        def sampleLine = "${sample.group},${sample.replicate},,${publishDir}/fastq_merged/${mergedFile},${fasta},${gtf}"
        """
        sampleLine="${sample.group},${sample.replicate},,${publishDir}/fastq_merged/${mergedFile},${fasta},${gtf}"
        """
        }


/*
 * set default parameters
 */



/*
 * define the workflow
 */

workflow{
        parquetFile = file(params.parquetpath)
        getParquet(parquetFile)
        setReferences(params.reference)
        def fasta = setReferences.out.fasta
        def gtf = setReferences.out.gtf
        def path = params.parquetpath
        def samples = getParquet.out
        .splitCsv(header: true)
        samples.view()
        def pairedChannel = samples.map { sampleRow -> [sampleRow, path] }

        // throw and error if the flowcell field is empty ?
        fileDir(pairedChannel)
        // commented out below to match all flowcell folders instead of from parquet metadata (works when empty field)
        fastqs = fileDir.out
        .map { barcode, baseDir, expFolder, flowcell ->
        //     "${baseDir}/${expFolder}/${expFolder}/*_${flowcell}_*/fastq_pass/barcode${barcode}"
            "${baseDir}/${expFolder}/${expFolder}/*/fastq_pass/barcode${barcode}"
        }
        fastqs.view()
        dir_files_ch = fastqs.map { dir -> tuple(dir, file("${dir}/*")) }
        mergeFiles(dir_files_ch)
        collectSampleInput(samples,mergeFiles.out,fasta,gtf,params.publishDir)
        Channel.value ('group,replicate,barcode,input_file,fasta,gtf')
        .concat( collectSampleInput.out)
        .collectFile(name: "${params.publishDir}/samplesheet_test.csv", sort: false, newLine: true)
}
