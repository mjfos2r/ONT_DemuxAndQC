version 1.0
# Some tasks pulled from github.com/broadinstitute/long-read-pipelines/wdl/tasks/Utility/GeneralUtils.wdl
# Others written by Michael J. Foster (github.com/mjfos2r)

import "../../structs/Structs.wdl"

task CompressTarPigz {
    meta {
        description: "compress files into a tarball using parallel gzip and generate an md5 checksum"
    }

    parameter_meta {
        files: "List of files to zip up."
        name: "Name of the tar.gz file."
    }

    input {
        Array[File] files
        String name

        Int num_cpus = 16
        Int mem_gb = 32
        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 50 + ceil(size(files, "GB"))*2

    command <<<
        NUM_CPUS=$(cat /proc/cpuinfo | awk '/^processor/{print }' | wc -l)
        set -euxo pipefail # crash out
        mkdir -p tarpit/
        for ff in ~{sep=' ' files}; do cp "${ff}" tarpit/ ; done
        tar -cf - -C tarpit/ . | pigz -p "${NUM_CPUS}" > ~{name}.tar.gz
        md5sum ~{name}.tar.gz >~{name}.tar.gz.md5
    >>>

    output {
        File tarball = "~{name}.tar.gz"
        File checksum = "~{name}.tar.gz.md5"
    }
    # How about you preempt these hands GCP.
    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          num_cpus,
        mem_gb:             mem_gb,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "mjfos2r/basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " SSD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task GetTodayDate {
    meta {
        description: "Generates a YYYY-MM-DD date of today (when this task is called). UTC."
        volatile: true
    }

    input {
        Int num_cpus = 1
        Int mem_gb = 4
        Int disk_size = 10
        RuntimeAttr? runtime_attr_override
    }

    command <<<
        date '+%Y-%m-%d'
    >>>

    output {
        String yyyy_mm_dd = read_string(stdout())
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          num_cpus,
        mem_gb:             mem_gb,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  2,
        max_retries:        1,
        docker:             "mjfos2r/basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " HDD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task ValidateMd5sum {
    meta {
        description: "simple task to validate checksum of a file"
    }

    parameter_meta {
        file: "input_file to validate"
        checksum: "file containing checksum"
    }

    input {
        File file
        File checksum

        RuntimeAttr? runtime_attr_override
    }

    # Estimate disk size - the compressed archive plus min disk size
    Int disk_size = 365 + ceil(size(file, "GB"))

    command <<<
    set -euxo pipefail # crash out

    EXPECTED_MD5=$(awk '{print $1}' "~{checksum}")
    # This is required as the awk command can hang.
    md5sum "~{file}" > actual_sum.txt
    ACTUAL_MD5=$(awk '{print $1}' actual_sum.txt)
    if [ "$EXPECTED_MD5" != "$ACTUAL_MD5" ]; then
        echo "ERROR: CHECKSUM VALIDATION FAILED FOR ~{file}"
        echo "ERROR: Expected: $EXPECTED_MD5"
        echo "ERROR:   Actual: $ACTUAL_MD5"
        echo "false" > valid.txt
    else
        echo "###################################################"
        echo "SUCCESS: Checksum validation successful for ~{file}"
        echo "###################################################"
        echo "true" > valid.txt
    fi
    >>>

    output {
        File is_valid = "valid.txt"
    }
    # DO NOT PREEMPT FTLOG.
    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          2,
        mem_gb:             8,
        disk_gb:            disk_size,
        boot_disk_gb:       10,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "mjfos2r/basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " SSD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task DecompressRunTarball {
    meta {
        description: "Decompress a validated run tarball using pigz"
    }

    parameter_meta {
        tarball: "validated tarball to decompress"
    }

    input {
        File tarball
        Boolean is_valid
        File? raw_hash
        File? raw_digest

        # Runtime parameters
        Int num_cpus = 16
        Int mem_gb = 64
        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 365 + 3*ceil(size(tarball, "GB"))

    command <<<
        set -euo pipefail

        NPROC=$(awk '/^processor/{print}' /proc/cpuinfo | wc -l)

        mkdir -p extracted
        mkdir -p merged

        echo "#####################################"
        echo "# RUN TARBALL IS VALID: ~{is_valid} #"
        echo "#####################################"

        if ! ~{is_valid}; then
            echo "ERROR: NOT ATTEMPTING DECOMPRESSION SINCE TARBALL IS CORRUPTED!"
            exit 1
        fi
        # todo: figure out a faster way to validate tarball integrity..should probably just do the md5sum checking here?
        # handy snippet from community post: 360073540652-Cromwell-execution-directory
        gcs_task_call_basepath=$(cat gcs_delocalization.sh | grep -o '"gs:\/\/.*/glob-.*/' | sed 's#^"##' |sed 's#/$##' | head -n 1)

        echo "$gcs_task_call_basepath"
        true > gcs_merged_bam_paths.txt

        # crack the tarball, strip the top bam_pass component so we're left with barcode dirs.
        tar --use-compress-program=pigz -xf ~{tarball} -C extracted --strip-components=1

        # if we've provided the hash and digest, validate em.
        if [[ -f "~{raw_digest}" && -f "~{raw_hash}" ]]; then
            echo "Validating raw tarball contents via provided raw_hash and raw_digest files"
            # if we have the raw_hash and raw_digest, check all the reads!
            md5sum -c "~{raw_digest}" || { echo "ERROR: Raw tarball digest validation failed!"; exit 1; }
            echo "SUCCESS: DIGEST CHECK"
            cd extracted
            md5sum -c ../"~{raw_hash}" || { echo "ERROR: Extracted reads validation failed!"; exit 1; }
            echo "SUCCESS: RAW FILES CHECK"
        else
            echo "no raw_hash/raw_digest provided, skipping individual file integrity checks!"
        fi

        # Get a list of our directories, pull the barcode ID, all so we can make a list of files for each
        find extracted -mindepth 1 -maxdepth 1 -type d | sort > directory_list.txt
        cut -d'/' -f2 directory_list.txt > barcodes.txt
        mkdir -p file_lists

        # Count the number of directories for verification
        wc -l directory_list.txt | awk '{print $1}' > directory_count.txt
        # Create/clear the counts file before the loop
        true > file_counts.txt

        # Lets hope this works and successfully merges either bams or fastqs.
        while read -r DIR_PATH; do
            BARCODE=$(basename "$DIR_PATH")
            # if we have bams, merge the bams.
            if find "$DIR_PATH" -name "*.bam" | grep -q .; then
                echo "BAM input detected. Merging using samtools"
                BAM_LIST="file_lists/${BARCODE}_files.txt"
                find "$DIR_PATH" -name "*.bam" | sort > "file_lists/${BARCODE}_files.txt"
                echo "${BARCODE} $(cat ${BAM_LIST} | wc -l)" >> file_counts.txt
                samtools merge -f -@ "$NPROC" -o merged/"${BARCODE}.merged.bam" -b "$BAM_LIST"
                echo "${gcs_task_call_basepath}/${BARCODE}.merged.bam" >> gcs_merged_reads_paths.txt

            elif find "$DIR_PATH" -name "*.fastq.gz" | grep -q .; then
                echo "Fastq input detected. Merging using SeqKit"
                FQ_LIST="file_lists/${BARCODE}_files.txt"
                find "$DIR_PATH" -name "*.fastq.gz" | sort > "file_lists/${BARCODE}_files.txt"
                echo "${BARCODE} $(cat ${FQ_LIST} | wc -l)" >> file_counts.txt
                cat "$DIR_PATH"/*.fastq.gz > merged/"${BARCODE}.merged.fastq"
                echo "${gcs_task_call_basepath}/${BARCODE}.merged.fastq" >> gcs_merged_reads_paths.txt
            else
                echo "ERROR: NO BAM OR FASTQ FILES FOUND IN $DIR_PATH"
            fi
        done < directory_list.txt
        >>>

    output {
        # how many barcodes we working with?
        Int directory_count = read_int("directory_count.txt")
        # how many read files we got?
        Array[Int] file_counts = read_lines("file_counts.txt")
        # output an array of our barcode_ids
        Array[String] barcode = read_lines("barcodes.txt")

        # output an array of our merged bam_files or fastqs
        Array[File] merged_reads = select_first([glob("merged/*.bam"), glob("merged/*.fastq")])
        File glob_paths = "gcs_merged_reads_paths.txt"
    }

    #########################
    # DO NOT PREEMPT THIS JOB FOR THE LOVE OF ALL THAT IS GOOD IN THIS WORLD.
    # Also use SSD please and thank you.
    RuntimeAttr default_attr = object {
        cpu_cores:          num_cpus,
        mem_gb:             mem_gb,
        disk_gb:            disk_size,
        boot_disk_gb:       50,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "mjfos2r/samtools:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " SSD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

task CreateMap {
    # gfys WDL1.0
    input {
        Array[String] keys
        Array[File] values
    }

    command <<<
        python3 <<CODE
        import json
        keys = ["~{sep='","' keys}"]
        values = ["~{sep='","' values}"]
        result = {}
        for i in range(len(keys)):
            result[keys[i]] = values[i]
        with open("map.json", "w") as f:
            json.dump(result, f)
        CODE
    >>>

    output {
        Map[String, File] mmap = read_json("map.json")
    }

    runtime {
        docker: "python:3.11-slim"
    }
}

task RenameFile {
    meta {
        description: "Decompress a validated run tarball using pigz"
    }

    parameter_meta {
        file: "file to rename"
        new_name: "new filename"
    }

    input {
        File file
        String new_name

        # Runtime parameters
        Int num_cpus = 2
        Int mem_gb = 8

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 50 + 2*ceil(size(file, "GB"))

    command <<<
        set -euxo pipefail

        mkdir -p renamed

        NEWNAME="~{new_name}"
        FILE="~{file}"
        EXT="${FILE##*.}"
        echo "Original Filename: ${FILE}"
        echo "Extension: ${EXT}"
        echo "New Filename: renamed/${NEWNAME}.${EXT}"
        mv "$FILE" "renamed/${NEWNAME}.${EXT}"
        >>>

    output {
        File renamed_file = glob("renamed/*")[0]
    }

    #########################
    # DO NOT PREEMPT THIS JOB FOR THE LOVE OF ALL THAT IS GOOD IN THIS WORLD.
    # Also use SSD please and thank you.
    RuntimeAttr default_attr = object {
        cpu_cores:          num_cpus,
        mem_gb:             mem_gb,
        disk_gb:            disk_size,
        boot_disk_gb:       15,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "mjfos2r/basic:latest"
    }
    RuntimeAttr runtime_attr = select_first([runtime_attr_override, default_attr])
    runtime {
        cpu:                    select_first([runtime_attr.cpu_cores,         default_attr.cpu_cores])
        memory:                 select_first([runtime_attr.mem_gb,            default_attr.mem_gb]) + " GiB"
        disks: "local-disk " +  select_first([runtime_attr.disk_gb,           default_attr.disk_gb]) + " SSD"
        bootDiskSizeGb:         select_first([runtime_attr.boot_disk_gb,      default_attr.boot_disk_gb])
        preemptible:            select_first([runtime_attr.preemptible_tries, default_attr.preemptible_tries])
        maxRetries:             select_first([runtime_attr.max_retries,       default_attr.max_retries])
        docker:                 select_first([runtime_attr.docker,            default_attr.docker])
    }
}

workflow GetGcpFileMd5 {
    # From broadinstitute/ops-terra-utils/wdl/GetGcpFileMd5.wdl
    input {
        String gcp_file_path
        Boolean create_cloud_md5_file
        String? md5_format
        String? docker
        Int? memory_gb
    }

    String docker_image = select_first([docker, "us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest"])

    call GetFileMd5 {
        input:
            gcp_file_path = gcp_file_path,
            create_cloud_md5_file = create_cloud_md5_file,
            docker_image = docker_image,
            md5_format = md5_format,
            memory_gb = memory_gb
    }

    output {
        String md5_hash = GetFileMd5.md5_hash
    }
}

task GetFileMd5 {
    # From broadinstitute/ops-terra-utils/wdl/GetGcpFileMd5.wdl
    input {
        String gcp_file_path
        Boolean create_cloud_md5_file
        String docker_image
        String? md5_format
        Int? memory_gb
    }

    command <<<
        python /etc/terra_utils/python/get_file_md5.py \
        --gcp_file_path ~{gcp_file_path} \
        --output_file object_md5.txt \
        ~{if create_cloud_md5_file then "--create_cloud_md5_file" else ""} \
        ~{"--md5_format " + md5_format}
    >>>

    runtime {
        docker: docker_image
        memory: select_first([memory_gb, 4]) + " GB"
    }

    output {
        String md5_hash = read_string("object_md5.txt")
    }
}