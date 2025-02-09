#!/usr/bin/env nextflow


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
                tuple val(sample),  env(formattedBarcode), env(barcodeFolder), env(mergedFile)


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
        def barcodeFolder = "${variable1}/fastq_pass/barcode${formattedBarcode}/*"


                """
                formattedBarcode=${formattedBarcode}
                barcodeFolder=${variable1}/fastq_pass/barcode${formattedBarcode}/*
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

// variables should be passed from this process to the workflow where the glob can be used to collect 
// all the files in each barcode directory


process mergeFiles{
        // here we use cat, ideally the input files are sorted based on numeric part in filename.
        container 'jbjespersen/parquet:test'
        input:
                tuple val(sample), val(formattedBarcode),path("${barcodeFolder}"), val(mergedFile)
        output:
                path "${mergedFile}"
        script:

        """
        echo "sample: ${sample.group} ${sample.replicate} ${formattedBarcode} ${sample.nucleic_acid_type}"
        cat ${barcodeFolder}/* >> ${mergedFile}
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

        // pairedChannel.view { println it }

        fileDir(pairedChannel)
        fileDir.out.view { println it }

        fastqs = fileDir.out.map{
                // row -> row.sample.group, row.sample.replicate, row.formattedBarcode
        }
        fastqs.view { }

        // mergeFiles(fastqs)

        // fastqs = sources.map { row -> row, 
        // // file("az://nextflowstorage/P29_41c4786a45264966bed24c7bd386873a/DNAseq/gridion/D29_3387863281cf4b70b45dcb5d91af68c8/DNFLRS7/DNFLRS7/20230425_1450_X2_FAP81332_d108c404/fastq_pass/barcode${row.sample_barcode}/FAP81332_pass_barcode${row.sample_barcode}_d108c404_99ac3298_*.fastq.gz" ,
        // file("/home/azureuser/blob/raw/P29_6f16f7434db544739603b3f030486642/DNAseq/gridion/D29_3387863281cf4b70b45dcb5d91af68c8/DNFLRS7/DNFLRS7/20230425_1450_X2_FAP81332_d108c404/fastq_pass/barcode${row.sample_barcode}/FAP81332_pass_barcode${row.sample_barcode}_d108c404_99ac3298_*.fastq.gz" ,
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




