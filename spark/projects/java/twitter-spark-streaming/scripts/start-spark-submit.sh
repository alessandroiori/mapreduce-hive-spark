#!/bin/bash

CONSUMER_KEY="Rl1rRTT4E4am8uV8DmPsJGjQL"
CONSUMER_SECRET="hhRlDnRvsif3LXhbP9VqcDGKw8cuSQrg3gW1vxGsyT5DczIX2i"
ACCESS_TOKEN="870559264800215040-RF1MAVh784ftFrwB0J1hy7SqcSEmZBR"
ACCESS_TOKEN_SECRET="M5YCKknLFKVsC07H3Dj3xfwuM90zmfMJ5lLxEnIzZ48Kn"
TOP_HASHTAGS_NUM=10

--packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0

spark-submit --class "it.uniroma3.TopNTwitterHashTag" \
--packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0 \
../target/twitter-spark-streaming-0.0.1.jar \
$CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET $TOP_HASHTAGS_NUM
