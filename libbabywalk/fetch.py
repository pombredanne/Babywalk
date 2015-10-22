import os
import os.path
import boto3
import logging
import subprocess
import tempfile


def _fetch(request, working_dir):

    depth = request['depth']
    seed = request['url']

    exts = {
        '.js', '.css', '.xml', '.atom', '.rss', '.jpeg', '.jpg', '.tif',
        '.gif', '.bmp', '.png', '.svg', '.ttf', '.woff', '.psb', '.psd',
        '.pdf', '.gz', '.bz2', '.zip', '.doc', '.docx', '.xls', '.xslx',
        '.ppt', '.pptx', '.swf', '.flac', '.fla', '.mp3', '.mp4', '.wav',
        '.wmv', '.mov'
    }

    cmd = ['wget', '--recursive', '--level={}'.format(depth), '--tries=5',
           '--dns-timeout=30', '--connect-timeout=5', '--read-timeout=5',
           '--timestamping', '--wait=5', '--random-wait', '--no-parent',
           '--no-verbose', '--no-check-certificate',
           '--reject=' + ','.join(exts),
           '--reject=' + ','.join(ext.upper() for ext in exts),
           '--warc-file=result', '--warc-tempdir=.', seed]

    logging.debug('execute %s', cmd)
    ec = subprocess.call(cmd,
                         cwd=working_dir,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    logging.info('finished crawling %s with %s', cmd[-1], ec)
    return os.path.join(working_dir, 'result.warc.gz')


def _upload(request, warc_file):

    if not os.path.exists(warc_file):
        return None

    with open(warc_file, mode='rb') as handle:
        boto3.resource('s3') \
             .Bucket(request['bucket']) \
             .put_object(Key=request['object'],
                         ContentType='application/x-gzip',
                         Body=handle.read())

    return 's3://{}/{}'.format(request['bucket'], request['object'])


def fetch_and_upload(requests, directory):

    for request in requests:
        with tempfile.TemporaryDirectory(dir=directory) as tmpdir:
            warcfile = _fetch(request['fetch'], tmpdir)
            result = _upload(request['upload'], warcfile)
            yield (request, result)
