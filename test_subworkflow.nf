//under development
include { INPUT_PREPROCESSING } from '../subworkflows/local/prepare_samplesheet/main.nf'

workflow TEST_SUBWORKFLOW {
    
    ch_parquet = Channel.fromPath(params.parquet_path)
        .map { path -> [['id': path.baseName], path] }
    
    INPUT_PREPROCESSING(
        ch_parquet,
        params.reference,
        params.baseRefPath,
        params.publishDir
    )
    
    INPUT_PREPROCESSING.out.samplesheet.view { meta, samplesheet ->
        "Generated samplesheet: ${samplesheet}"
    }
}