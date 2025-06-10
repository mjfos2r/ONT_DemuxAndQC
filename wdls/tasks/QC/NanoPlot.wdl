version 1.0
import "../../structs/Structs.wdl"
# From github.com/broadinstitute/long-read-pipeline

task NanoPlotFromSummary {

    meta {
        description: "Use NanoPlot to generate plots from ONT summary files"
    }

    parameter_meta {
        summary_files: "Dorado generated sequencing summary file(s) to use as input"
        is_valid: "Array of booleans for each summary indicating file integrity."
        runtime_attr_override: "Override the default runtime attributes"
    }

    input {
        Array[File] summary_files
        Array[Boolean] is_valid
        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 100 + 2*ceil(size(summary_files, "GB"))

    command <<<
        set -euxo pipefail

        echo "#######################################################"
        echo "# SEQUENCING SUMMARIES ARE VALID: ~{sep=',' is_valid} #"
        echo "#######################################################"

        NPROCS=$(cat /proc/cpuinfo | grep '^processor' | tail -n1 | awk '{print $NF+1}' )

        mkdir -p nanoplots/barcoded nanoplots/overall

        # generate barcode specific reports and plots
        NanoPlot -t "${NPROCS}" \
                --outdir nanoplots/barcoded \
                -c royalblue \
                --N50 \
                --tsv_stats \
                --barcoded \
                --summary ~{sep=' ' summary_files}

        # generate overall reports and plots
        NanoPlot -t "${NPROCS}" \
                --outdir nanoplots/overall \
                 -c royalblue \
                 --N50 \
                 --tsv_stats \
                 --summary ~{sep=" " summary_files}

        # Pull the metrics from the overall stats, (both are identical but pick this one.)
        grep -v -e '^Metrics' -e '^highest' -e '^longest' nanoplots/overall/NanoStats.txt | \
            sed 's/ >/_/' | \
            sed 's/://' | \
            awk '{ print $1 "\t" $2 }' | \
            tee map.txt

        #make a tarball of all nanoplot reports and output that as a single file.
        tar -zcf nanoplots.tar.gz nanoplots
    >>>

    #number_of_reads 88000
    #number_of_bases 467855516.0
    #median_read_length      4086.0
    #mean_read_length        5316.5
    #read_length_stdev       4413.2
    #n50     6731.0
    #active_channels 506
    #mean_qual       12.8
    #median_qual     13.7
    #Reads_Q5        85483
    #Reads_Q7        80249
    #Reads_Q10       71810
    #Reads_Q12       59097
    #Reads_Q15       26597

    output {
        File tarball = "nanoplots.tar.gz"
        File map = "map.txt"
        Map[String, Float] stats_map = read_map("map.txt")
        #File ActivePores_Over_Time = "ActivePores_Over_Time.png"
        #File ActivityMap_ReadsPerChannel = "ActivityMap_ReadsPerChannel.png"
        #File CumulativeYieldPlot_Gigabases = "CumulativeYieldPlot_Gigabases.png"
        #File CumulativeYieldPlot_NumberOfReads = "CumulativeYieldPlot_NumberOfReads.png"
        #File LengthvsQualityScatterPlot_dot = "LengthvsQualityScatterPlot_dot.png"
        #File LengthvsQualityScatterPlot_kde = "LengthvsQualityScatterPlot_kde.png"
        #File Non_weightedHistogramReadlength = "Non_weightedHistogramReadlength.png"
        #File Non_weightedLogTransformed_HistogramReadlength = "Non_weightedLogTransformed_HistogramReadlength.png"
        #File NumberOfReads_Over_Time = "NumberOfReads_Over_Time.png"
        #File TimeLengthViolinPlot = "TimeLengthViolinPlot.png"
        #File TimeQualityViolinPlot = "TimeQualityViolinPlot.png"
        #File WeightedHistogramReadlength = "WeightedHistogramReadlength.png"
        #File WeightedLogTransformed_HistogramReadlength = "WeightedLogTransformed_HistogramReadlength.png"
        #File Yield_By_Length = "Yield_By_Length.png"
    }

    #########################
    # BEGONE PREEMPTION
    RuntimeAttr default_attr = object {
        cpu_cores:          8,
        mem_gb:             32,
        disk_gb:            disk_size,
        boot_disk_gb:       50,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "quay.io/biocontainers/nanoplot:1.35.5--pyhdfd78af_0"
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

task NanoPlotFromRichFastqs {

    meta {
        description: "Use NanoPlot to generate plots from a list of ONT fastq files"
    }

    parameter_meta {
        fastqs: "A list of fastq files to use as input"
        runtime_attr_override: "Override the default runtime attributes"
    }

    input {
        Array[File] fastqs

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 2*ceil(size(fastqs, "GB"))

    command <<<
        set -euo pipefail

        NPROCS=$( grep '^processor' /proc/cpuinfo | tail -n1 | awk '{print $NF+1}' )

        NanoPlot -t "${NPROCS}" \
                 --N50 \
                 --tsv_stats \
                 --fastq_rich "~{sep=' ' fastqs}"

        grep -v -e '^Metrics' -e '^highest' -e '^longest' NanoStats.txt | \
            sed 's/ >/_/' | \
            sed 's/://' | \
            awk '{ print $1 "\t" $2 }' | \
            tee map.txt
    >>>

    output {
        File stats = "NanoStats.txt"
        Map[String, Float] stats_map = read_map("map.txt")

        Array[File] plots = glob("*.png")
        #File ActivePores_Over_Time = "ActivePores_Over_Time.png"
        #File ActivityMap_ReadsPerChannel = "ActivityMap_ReadsPerChannel.png"
        #File CumulativeYieldPlot_Gigabases = "CumulativeYieldPlot_Gigabases.png"
        #File CumulativeYieldPlot_NumberOfReads = "CumulativeYieldPlot_NumberOfReads.png"
        #File LengthvsQualityScatterPlot_dot = "LengthvsQualityScatterPlot_dot.png"
        #File LengthvsQualityScatterPlot_kde = "LengthvsQualityScatterPlot_kde.png"
        #File Non_weightedHistogramReadlength = "Non_weightedHistogramReadlength.png"
        #File Non_weightedLogTransformed_HistogramReadlength = "Non_weightedLogTransformed_HistogramReadlength.png"
        #File NumberOfReads_Over_Time = "NumberOfReads_Over_Time.png"
        #File TimeLengthViolinPlot = "TimeLengthViolinPlot.png"
        #File TimeQualityViolinPlot = "TimeQualityViolinPlot.png"
        #File WeightedHistogramReadlength = "WeightedHistogramReadlength.png"
        #File WeightedLogTransformed_HistogramReadlength = "WeightedLogTransformed_HistogramReadlength.png"
        #File Yield_By_Length = "Yield_By_Length.png"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          4,
        mem_gb:             32,
        disk_gb:            disk_size,
        boot_disk_gb:       25,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "quay.io/biocontainers/nanoplot:1.35.5--pyhdfd78af_0"
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

task NanoPlotFromBams {

    meta {
        description: "Use NanoPlot to generate plots from a bam file"
    }

    parameter_meta {
        bams: "An array of bam files to use as input"
        bais: "The bai file for the bam file"
        runtime_attr_override: "Override the default runtime attributes"
    }

    input {
        Array[File] bams # feed it the sorted and merged files I guess?
        Array[File] bais

        RuntimeAttr? runtime_attr_override
    }

    Int disk_size = 10 + 2*ceil(size(bams, "GB")) + 2*ceil(size(bais, "GB"))

    command <<<
        set -euxo pipefail

        touch "~{sep=' ' bais}" # avoid the warning bai is older than bam

        NPROCS=$( grep '^processor' /proc/cpuinfo | tail -n1 | awk '{print $NF+1}' )

        NanoPlot -t "${NPROCS}" \
                 --N50 \
                 --tsv_stats \
                 --no_supplementary \
                 --verbose \
                 --bam "~{sep=' ' bams}"

        grep -v -e '^Metrics' -e '^highest' -e '^longest' NanoStats.txt | \
            sed 's/ >/_/' | \
            sed 's/://' | \
            awk '{ print $1 "\t" $2 }' | \
            tee map.txt
    >>>

    #number_of_reads 143488
    #number_of_bases 993469297.0
    #number_of_bases_aligned 402067275.0
    #fraction_bases_aligned  0.4
    #median_read_length      5081.0
    #mean_read_length        6923.7
    #read_length_stdev       6116.7
    #n50     9210.0
    #average_identity        92.8
    #median_identity 94.5
    #mean_qual       14.6
    #median_qual     15.0
    #Reads_Q5        143488
    #Reads_Q7        143488
    #Reads_Q10       140551
    #Reads_Q12       119386
    #Reads_Q15       71164

    output {
        File stats = "NanoStats.txt"
        Map[String, Float] stats_map = read_map("map.txt")

        Array[File] plots = glob("*.png")
#        File AlignedReadlengthvsSequencedReadLength_dot = "AlignedReadlengthvsSequencedReadLength_dot.png"
#        File AlignedReadlengthvsSequencedReadLength_kde = "AlignedReadlengthvsSequencedReadLength_kde.png"
#        File LengthvsQualityScatterPlot_dot = "LengthvsQualityScatterPlot_dot.png"
#        File LengthvsQualityScatterPlot_kde = "LengthvsQualityScatterPlot_kde.png"
#        File MappingQualityvsAverageBaseQuality_dot = "MappingQualityvsAverageBaseQuality_dot.png"
#        File MappingQualityvsAverageBaseQuality_kde = "MappingQualityvsAverageBaseQuality_kde.png"
#        File MappingQualityvsReadLength_dot = "MappingQualityvsReadLength_dot.png"
#        File MappingQualityvsReadLength_kde = "MappingQualityvsReadLength_kde.png"
#        File Non_weightedHistogramReadlength = "Non_weightedHistogramReadlength.png"
#        File Non_weightedLogTransformed_HistogramReadlength = "Non_weightedLogTransformed_HistogramReadlength.png"
#        File PercentIdentityHistogramDynamic_Histogram_percent_identity = "PercentIdentityHistogramDynamic_Histogram_percent_identity.png"
#        File PercentIdentityvsAlignedReadLength_dot = "PercentIdentityvsAlignedReadLength_dot.png"
#        File PercentIdentityvsAlignedReadLength_kde = "PercentIdentityvsAlignedReadLength_kde.png"
#        File PercentIdentityvsAverageBaseQuality_dot = "PercentIdentityvsAverageBaseQuality_dot.png"
#        File PercentIdentityvsAverageBaseQuality_kde = "PercentIdentityvsAverageBaseQuality_kde.png"
#        File WeightedHistogramReadlength = "WeightedHistogramReadlength.png"
#        File WeightedLogTransformed_HistogramReadlength = "WeightedLogTransformed_HistogramReadlength.png"
#        File Yield_By_Length = "Yield_By_Length.png"
    }

    #########################
    RuntimeAttr default_attr = object {
        cpu_cores:          8,
        mem_gb:             24,
        disk_gb:            disk_size,
        boot_disk_gb:       25,
        preemptible_tries:  0,
        max_retries:        1,
        docker:             "us.gcr.io/terra-942df462/nanoplot:latest" # need to get specific tag
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
