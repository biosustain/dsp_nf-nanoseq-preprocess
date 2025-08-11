workflow prepare_samplesheet{
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