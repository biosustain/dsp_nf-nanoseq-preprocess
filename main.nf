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

//inputPath can be used more than once?
process fileDir{
        // should be easy bash code, more simple container can be used
        container 'jbjespersen/parquet:test'
        input:
                val inputPath
        output:
                path "string.txt"
        script:
        def path = inputPath
        def regex1 = /^(.+?)\/fastq_pass/
        def regex2 = /([^\/]+)_barcode/
        def regex3 = /_barcode[0-9]+_(.+?)_[0-9]+\.fastq\.gz$/

        def variable1 = (path =~ regex1)[0][1]
        def variable2 = (path =~ regex2)[0][1]
        def variable3 = (path =~ regex3)[0][1]

        """
                echo "Input dir" >> string.txt
                echo "${variable1}/fastq_pass/barcodeXX/" >>string.txt
                echo "Input file" >> string.txt
                echo "${variable1}/fastq_pass/barcodeXX/${variable2}_barcodeXX_${variable3}_N.fastq.gz" >>string.txt
                echo "Output file" >> string.txt
                echo "${variable1}/${variable2}_barcodeXX_${variable3}_merged.fastq.gz" >>string.txt
        """
}

process sampleBarcode{
        // should be easy bash code, more simple container can be used
        container 'jbjespersen/parquet:test'
        input:
                val sample
        output:
                path "formattedBarcode.txt"
        script:

        def formattedBarcode = String.format("%02d", sample.sample_barcode as Integer)

        """
                echo "${formattedBarcode}" > formattedBarcode.txt
        """
}


/// need to emit? and collectTuple? 

// variables should be passed from this process to the workflow where the glob can be used to collect 
// all the files in each barcode directory


process mergeFiles{
        // here we use cat, ideally the input files are sorted based on numeric part in filename.
        container 'jbjespersen/parquet:test'
        input:
                val
        output:
                val
        script:

        """

        """
}



workflow{
        parquetFile = file(params.parquetpath)
        getParquet(parquetFile)
        barcodeNumber(getParquet.out)

        // here we map the content of sample sheet:
        samples = barcodeNumber.out
        .splitCsv(header: true)
        
        // sample.sample_barcode.format("%02d", sample.sample_barcode as Integer)
        def string = Channel.of(params.fastqsplit)
        fileDir(string)
        //sampleBarcode(samples)
        
}




