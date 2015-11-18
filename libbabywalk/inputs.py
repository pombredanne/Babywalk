import re
import csv
import boto3
import codecs


def read_seeds_from_s3(path, filename):

    uri = path + '/' + filename
    bucket, name = re.match(r's3://(.*)/(.*)', uri).groups()

    response = boto3.resource('s3').Object(bucket, name).get()
    if response:
        codec = codecs.getreader('utf-8')
        handle = codec(response.get('Body'), 'ignore')

        return (row for row in csv.DictReader(handle, delimiter='\t'))


def read_seeds_from_file(filename):

    with open(filename, mode='r', encoding='utf-8') as handle:
        reader = csv.DictReader(handle, delimiter='\t')
        for row in reader:
            yield row
