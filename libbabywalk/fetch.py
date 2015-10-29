import os
import os.path
import logging
import subprocess
import tempfile


def fetch_warc(request, working_dir):

    depth = request['depth']
    seed = request['url']

    exts = {
        '.js', '.css', '.xml', '.atom', '.rss', '.jpeg', '.jpg', '.tif',
        '.gif', '.bmp', '.png', '.svg', '.ttf', '.woff', '.psb', '.psd',
        '.pdf', '.gz', '.bz2', '.zip', '.doc', '.docx', '.xls', '.xslx',
        '.ppt', '.pptx', '.swf', '.flac', '.fla', '.mp3', '.mp4', '.wav',
        '.wmv', '.mov'
    }

    cmd = ['wget', '--quiet', '--recursive', '--no-parent', '--timestamping',
           '--wait=5', '--random-wait', '--tries=5', '--dns-timeout=30',
           '--connect-timeout=5', '--read-timeout=5', '--no-check-certificate',
           '--level={}'.format(depth), '--reject=' + ','.join(exts),
           '--reject=' + ','.join(ext.upper() for ext in exts),
           '--warc-file=result', '--warc-tempdir=.', seed]

    logging.debug('execute %s', cmd)
    ec = subprocess.call(cmd, cwd=working_dir, timeout=3600)
    logging.debug('finished crawling %s with %s', cmd[-1], ec)
    return os.path.join(working_dir, 'result.warc.gz')


def _upload(request, result_file, aws_s3):

    if not os.path.exists(result_file):
        return None

    with open(result_file, mode='rb') as handle:
        aws_s3.Bucket(request['bucket']) \
              .put_object(Key=request['object'],
                          ContentType='application/x-gzip',
                          Body=handle.read())

    return 's3://{}/{}'.format(request['bucket'], request['object'])


def fetch_and_upload(requests, directory, aws_s3):

    for request in requests:
        try:
            with tempfile.TemporaryDirectory(dir=directory) as tmpdir:
                warcfile = fetch_warc(request['fetch'], tmpdir)
                result = _upload(request['upload'], warcfile, aws_s3)
        except Exception as ex:
            logging.exception('failed to crawl: "%s"', request['fetch']['url'])
            result = 'failed to crawl: {}'.format(str(ex))
        finally:
            yield (request, result)
