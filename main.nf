#!/usr/bin/env nextflow


// set default baseRefPath = "az://nextflowstorage/references/" ?


process getParquet{
        container 'jbjespersen/parquet:test'
        input:
                path parquetpath
        output:
                path "sample_data.csv"
        script:
        """
        parquet-tools csv --columns group,replicate,sample_barcode,nucleic_acid_type nanopore_sequencing_submission_sample.parquet > sample_data.csv
        """
}


// detect file exists and make string of path
process setReferences{
        container 'jbjespersen/parquet:test'
        input:
                val referenceID
        output:
                env (fasta), emit: fasta
                env (gtf), emit: gtf
        script:
        def baseRefPath = "az://nextflowstorage/references/"
        """
        fasta=${baseRefPath}${referenceID}.fasta
        gtf=${baseRefPath}${referenceID}.gtf
        """
}

// also get parquetpath as string in order to generate base path for other relevant files.


process barcodeNumber{
        container 'jbjespersen/parquet:test'
        input:
                path "sample_data.csv"
        output:
                path "sample_data_1.csv"
        script:
        """
                head -n 1 sample_data.csv > sample_data_1.csv
                awk -F ',' -v OFS=',' 'FNR == 1 {next} { sub("[a-z]+", "", \$3); print }' sample_data.csv >> sample_data_1.csv
        """
}




// structure to match from the parquet file and to reconstruct the file paths for the fastq files:
// ${basedir}/${foldername}/benchling/nanopore_sequencing_submission_sample.parquet
// ${basedir}/${foldername}/${foldername}/*_X${params.flowcell}_*/fastq_pass/barcode${barcode}/*.fastq.gz
//                                        |                     |                              |
//                                        timestamp             ID1_ID2                        ID1_pass_barcode_ID2_ID3
//                                       /${flowcell_folder}


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
        def flowcell = params.flowcell
        def formattedBarcode = String.format("%02d", sample.sample_barcode as Integer)

        

                """
                formattedBarcode=${formattedBarcode}
                baseDir=${baseDir}
                expFolder=${expFolder}
                flowcell=${params.flowcell}

                """

}


process mergeFiles{
        // here we use cat, ideally the input files are sorted based on numeric part in filename.
        container 'jbjespersen/parquet:test'
        publishDir(
           path: "${params.publishDir}/fastq_merged",
           overwrite: true,
                mode: 'copy'
        )
        input:

                tuple val(dir), path(files)

        output:

                path "merged_${dir.replaceAll('.*/', '')}.fastq.gz"
        script:


        """
        cat ${files} > merged_${dir.replaceAll('.*/', '')}.fastq.gz
        """
}

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

process finalizeSamplesheet{
        container 'jbjespersen/parquet:test'
        publishDir(
           path: "${params.publishDir}",
           overwrite: true,
                mode: 'copy'
        )
        input:
                val(sampleLine)
        output:
                path "samplesheet.csv"
        script:
        """
        echo "group,replicate,barcode,input_file,fasta,gtf" > samplesheet.csv
        echo "${sampleLine}" >> samplesheet.csv
        """
        }


workflow{
        parquetFile = file(params.parquetpath)
        getParquet(parquetFile)
        barcodeNumber(getParquet.out)

        def path = params.parquetpath

        def samples = barcodeNumber.out
        .splitCsv(header: true)

        def pairedChannel = samples.map { sampleRow -> [sampleRow, path] }

        fileDir(pairedChannel)

        fastqs = fileDir.out
        .map { barcode, baseDir, expFolder, flowcell  ->
            "${baseDir}/${expFolder}/${expFolder}/*_X${flowcell}_*/fastq_pass/barcode${barcode}"
        }

        dir_files_ch = fastqs.map { dir -> tuple(dir, file("${dir}/*")) }
        
        mergeFiles(dir_files_ch)


        mergedFiles = mergeFiles.out
        collectSampleInput(samples,mergedFiles,params.fasta,params.gtf,params.publishDir)
        sampleLines = collectSampleInput.out.collect().map { it.join("\n") }
        finalizeSamplesheet(sampleLines)

}




