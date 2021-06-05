

##To view the tree structure of the data
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -ls -R /data/msd | awk '{print $8}' | \sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
 |---audio
 |-----attributes
 |-------msd-jmir-area-of-moments-all-v1.0.attributes.csv
 |-------msd-jmir-lpc-all-v1.0.attributes.csv
 |-------msd-jmir-methods-of-moments-all-v1.0.attributes.csv
 |-------msd-jmir-mfcc-all-v1.0.attributes.csv
 |-------msd-jmir-spectral-all-all-v1.0.attributes.csv
 |-------msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
 |-------msd-marsyas-timbral-v1.0.attributes.csv
 |-------msd-mvd-v1.0.attributes.csv
 |-------msd-rh-v1.0.attributes.csv
 |-------msd-rp-v1.0.attributes.csv
 |-------msd-ssd-v1.0.attributes.csv
 |-------msd-trh-v1.0.attributes.csv
 |-------msd-tssd-v1.0.attributes.csv
 |-----features
 |-------msd-jmir-area-of-moments-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-jmir-lpc-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-jmir-methods-of-moments-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-jmir-mfcc-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-jmir-spectral-all-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-jmir-spectral-derivatives-all-all-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-marsyas-timbral-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-mvd-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-rh-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-rp-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-ssd-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-trh-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-------msd-tssd-v1.0.csv
 |---------part-00000.csv.gz
 |---------part-00001.csv.gz
 |---------part-00002.csv.gz
 |---------part-00003.csv.gz
 |---------part-00004.csv.gz
 |---------part-00005.csv.gz
 |---------part-00006.csv.gz
 |---------part-00007.csv.gz
 |-----statistics
 |-------sample_properties.csv.gz
 |---genre
 |-----msd-MAGD-genreAssignment.tsv
 |-----msd-MASD-styleAssignment.tsv
 |-----msd-topMAGD-genreAssignment.tsv
 |---main
 |-----summary
 |-------analysis.csv.gz
 |-------metadata.csv.gz
 |---tasteprofile
 |-----mismatches
 |-------sid_matches_manually_accepted.txt
 |-------sid_mismatches.txt
 |-----triplets.tsv
 |-------part-00000.tsv.gz
 |-------part-00001.tsv.gz
 |-------part-00002.tsv.gz
 |-------part-00003.tsv.gz
 |-------part-00004.tsv.gz
 |-------part-00005.tsv.gz
 |-------part-00006.tsv.gz
 |-------part-00007.tsv.gz

##To view the line counts of every dataset

# Line Count for attributes dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ for i in `hdfs dfs -ls /data/msd/audio/attributes | awk '{print $8}'` ; do echo $i ; hdfs dfs -cat $i | wc -l; done
/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv
21
/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv
21
/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv
11
/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv
27
/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv
17
/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
17
/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv
125
/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv
421
/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv
61
/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv
1441
/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv
169
/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv
421
/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv
1177

#Lines count for songs in features dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$  for i in `hdfs dfs -ls /data/msd/audio/features | awk '{print $8}'` ; do echo $i ; hdfs dfs -cat $i/* | gunzip | wc -l; done
/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
994623
/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
994623
/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
994623
/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
994623
/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
994623
/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
994623
/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
995001
/data/msd/audio/features/msd-mvd-v1.0.csv
994188
/data/msd/audio/features/msd-rh-v1.0.csv
994188
/data/msd/audio/features/msd-rp-v1.0.csv
994188
/data/msd/audio/features/msd-ssd-v1.0.csv
994188
/data/msd/audio/features/msd-trh-v1.0.csv
994188
/data/msd/audio/features/msd-tssd-v1.0.csv
994188

#Line Count for files inside Statistics dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -cat /data/msd/audio/statistics/* | gunzip |wc -l
992866

#Line Count for files in Song Genre dataset

[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ for i in `hdfs dfs -ls /data/msd/genre | awk '{print $8}'` ; do echo $i ; hdfs dfs -cat $i | wc -l; done
/data/msd/genre/msd-MAGD-genreAssignment.tsv
422714
/data/msd/genre/msd-MASD-styleAssignment.tsv
273936
/data/msd/genre/msd-topMAGD-genreAssignment.tsv
406427


#Line Count for files in Summary dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ for i in `hdfs dfs -ls /data/msd/main/summary | awk '{print $8}'` ; do echo $i ; hdfs dfs -cat $i | gunzip | wc -l; done
/data/msd/main/summary/analysis.csv.gz
1000001
/data/msd/main/summary/metadata.csv.gz
1000001

#Line Count for files in the tasteprofile mismatches dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ for i in `hdfs dfs -ls /data/msd/tasteprofile/mismatches | awk '{print $8}'` ; do echo $i ; hdfs dfs -cat $i | wc -l; done
/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt
938
/data/msd/tasteprofile/mismatches/sid_mismatches.txt
19094

#Line Count for the triplets dataset
[gsa59@canterbury.ac.nz@mathmadslinux1p ~]$ hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/* | gunzip |wc -l
48373586

