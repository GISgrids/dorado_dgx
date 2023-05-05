import pysam
import argparse
import os
import sys
from pathlib import Path
from statistics import mean
#from pysam.libcalignmentfile cimport AlignmentFile, AlignedSegment

def parse_args():
    # arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_bam", required=True, default=None,
                        help="Input BAM file. Default: None.")
    parser.add_argument("--output_dir", required=False, default=".",
                        help="Output dir. Default: .") 
    parser.add_argument("--threads", required=False, default="1",
                        help="Number of threads to use. Default: 1")                           
    args = parser.parse_args()
    # checks
    if not os.path.exists(args.input_bam):
        logging.error("Input file not found: %s" % args.input_file)
        sys.exit(1)

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    return args

def get_name( bam , out_dir ):
    path = Path(bam)
    sample_name = path.stem
    print("Processing BAM: %s" % bam)
    out_name = "%s/%s.%s" % ( out_dir , sample_name , "seq_summary.txt")
    return out_name

def get_BAM_read_length_quality(bam , out_name ):
    out_file = open( out_name , "w" )
    bamfile = pysam.AlignmentFile( bam , "rb" , check_sq=False )
    for reads in bamfile:
        read_qual = 0
        if reads.query_length < 2 :
            read_qual = 0
        else:
            read_qual = float(mean(reads.query_qualities))
        reads_info = "%s\t%d\t%f\n" % (reads.query_name , reads.query_length , read_qual ) 
        out_file.write(reads_info)
    out_file.close()
    return out_file
  
if __name__ == "__main__":
    args = parse_args()
    
    out_name = get_name(args.input_bam , args.output_dir )
    out_file = get_BAM_read_length_quality(args.input_bam , out_name )
    