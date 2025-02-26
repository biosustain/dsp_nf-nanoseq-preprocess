#!/usr/bin/env nextflow


// reference should be passed as a string, and file paths should be generated to be included in sample sheet.


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

// also get parquetpath as string in order to generate base path for other relevant files.



// put barcode digits and barcode number in separate columns in temp csv file.
// make barcodedigits column
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


// the following should be be simplified to short workflow parsing
// for now it is a way to get filepaths to put into samplesheet
// get full dir until fastq_pass/
// barcode folder not relevant at this moment
// file name should be broken down to replace barcode numbers, and also to iterate through fastqfiles.

// define output tuple to get relevant info for remaining part of pipeline
process fileDir{
        // should be easy bash code, more simple container can be used
        container 'jbjespersen/parquet:test'
        input:
                tuple val(sample), val(inputPath)
                
        output:
                // path "string.txt"
                // tuple val(sample), path("${variable1}/${variable2}_barcode${formattedBarcode}_${variable3}_merged.fastq.gz")
                // tuple val(sample), val(xyz), path("string.txt")
                // tuple val(sample),  env(formattedBarcode), env(barcodeFolder), env(mergedFile)
                // val(sample), emit: sampleInfo
                tuple val(sample), env(formattedBarcode), env(mergedFile), emit: sampleInfo
                env(barcodeFolder), emit: fastqDir


        script:
        def path = inputPath
        def regex1 = /^(.+?)\/fastq_pass/
        def regex2 = /([^\/]+)_barcode/
        def regex3 = /_barcode[0-9]+_(.+?)_[0-9]+\.fastq\.gz$/

        def variable1 = (path =~ regex1)[0][1]
        def variable2 = (path =~ regex2)[0][1]
        def variable3 = (path =~ regex3)[0][1]

        def formattedBarcode = String.format("%02d", sample.sample_barcode as Integer)

        def mergedFile = "${variable2}_barcode${formattedBarcode}_${variable3}_merged.fastq.gz"
        def barcodeFolder = "${variable1}/fastq_pass/barcode${formattedBarcode}"


                """
                formattedBarcode=${formattedBarcode}
                barcodeFolder=${variable1}/fastq_pass/barcode${formattedBarcode}
                echo "Input dir" >> string.txt
                echo "${variable1}/fastq_pass/barcode${formattedBarcode}/" >>string.txt
                echo "Input file" >> string.txt
                echo "${variable1}/fastq_pass/barcode${formattedBarcode}/${variable2}_barcode${formattedBarcode}_${variable3}_N.fastq.gz" >>string.txt
                echo "Output file" >> string.txt
                echo "${variable1}/${variable2}_barcode${formattedBarcode}_${variable3}_merged.fastq.gz" >>string.txt
                mergedFile="${variable2}_barcode${formattedBarcode}_${variable3}_merged.fastq.gz"
                echo ${mergedFile}
                """

}

// need to be updated with flow cell metadata
// variables should be passed from this process to the workflow where the glob can be used to collect 
// all the files in each barcode directory


//very likely we have to use the `each` connotation to flatten the input data.
process mergeFiles{
        // here we use cat, ideally the input files are sorted based on numeric part in filename.
        container 'jbjespersen/parquet:test'
        input:
                tuple val(dir), path(files)
                // path("*.fastq.gz")
                // tuple val(sample), val(formattedBarcode),path(barcodeFolder), val(mergedFile)
        output:
                // path "merged_output.fastq.gz"
                path "merged_${dir.replaceAll('.*/', '')}.fastq.gz"
        script:
        // cat ${files} > merged_output.fastq.gz

        """
        cat ${files} > merged_${dir.replaceAll('.*/', '')}.fastq.gz
        """
}
        // #echo "sample: ${sample.group} ${sample.replicate} ${formattedBarcode} ${sample.nucleic_acid_type}"
        // #echo "${barcodeFolder}"
        // #echo "${mergedFile}"
        // #cat ${barcodeFolder} >> ${mergedFile}

//  need to make sure one iunstead of 7 processes are created
process finalizeSamplesheet{
        container 'jbjespersen/parquet:test'
        input:
                val all_tuples
                // each tuple val(sample), val(formattedBarcode), val(mergedFile)
        output:
                path "samplesheet.csv"
        script:
        """
        echo "group,replicate,barcode,input_file,fasta,gtf" > samplesheet.csv
        ${all_tuples.collect { row -> "${row[0]}" }.join("\n")} >> samplesheet.csv
        """
        }


workflow{
        parquetFile = file(params.parquetpath)
        getParquet(parquetFile)
        barcodeNumber(getParquet.out)



        def path = params.fastqsplit

        // here we map the content of sample sheet:
        def samples = barcodeNumber.out
        .splitCsv(header: true)

        def pairedChannel = samples.map { sampleRow -> [sampleRow, path] }

        fileDir(pairedChannel)

        fastqs = fileDir.out.fastqDir

        dir_files_ch = fastqs.map { dir -> tuple(dir, file("${dir}/*")) }
        
        mergeFiles(dir_files_ch)


        // fileDir.out.sampleInfo.collect().subscribe {
        //         all_tuples ->
        //         println "collected tuples: ${all_tuples}"
        // }
        // finalizeSamplesheet(fastqs)
        // finalizeSamplesheet(fileDir.out.sampleInfo.collect())
        // fastqs = sources.map { row -> row, 
        //         checkIfExists: true)
        // } 
        // groupTuple
        // grouped_fastqs = fastqs.map { meta, fastq ->
        //         meta.id, meta, fastq
        // }
        // .groupTuple()
        // .map { id, meta, fastqs ->
        //         [ meta, fastqs ]
        // }
        // fileDir(${params.fastqsplit},val from sample_data.csv)
}
