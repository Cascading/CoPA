#!/bin/bash -ex
# edit the `BUCKET` variable to use one of your S3 buckets:
BUCKET=temp.cascading.org/copa
SINK=out

# clear previous output (required by Apache Hadoop)
s3cmd del -r s3://$BUCKET/$SINK

# load built JAR + input data
s3cmd put target/copa.jar s3://$BUCKET/
s3cmd put --recursive data s3://$BUCKET/

# launch cluster and run
elastic-mapreduce --create --name "CoPA" \
  --debug --enable-debugging --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/copa.jar \
  --arg s3n://$BUCKET/data/copa.csv \
  --arg s3n://$BUCKET/data/meta_tree.tsv \
  --arg s3n://$BUCKET/data/meta_road.tsv \
  --arg s3n://$BUCKET/data/gps.csv \
  --arg s3n://$BUCKET/$SINK/trap \
  --arg s3n://$BUCKET/$SINK/park \
  --arg s3n://$BUCKET/$SINK/tree \
  --arg s3n://$BUCKET/$SINK/road \
  --arg s3n://$BUCKET/$SINK/shade \
  --arg s3n://$BUCKET/$SINK/gps \
  --arg s3n://$BUCKET/$SINK/reco
