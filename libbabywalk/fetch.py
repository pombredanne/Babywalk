import os
import os.path
import shutil
import boto3
import logging
import subprocess
import uuid


class Content(object):
    def __init__(self, directory, uid):
        self.name = os.path.join(directory, uid)
        self.content = os.path.join(self.name, 'content')
        self.result = os.path.join(self.name, 'result.warc.gz')
        os.mkdir(self.name)

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        if self.name is not None:
            shutil.rmtree(self.name)

    def fetch(self, seed, depth):
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
               '--warc-file', os.path.join(self.name, 'result'),
               '--warc-tempdir=.', seed]

        logging.debug('execute %s', cmd)
        ec = subprocess.call(cmd,
                             cwd=self.content,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        logging.info('finished crawling %s with %s', cmd[-1], ec)

    def upload(self, bucket, filename):
        if not os.path.exists(self.result):
            return None

        with open(result_path, mode='rb') as handle:
            boto3.resource('s3') \
                 .Bucket(bucket) \
                 .put_object(Key=filename,
                             ContentType='application/x-gzip',
                             Body=handle.read())

        return 's3://{}/{}'.format(bucket, filename)

    @staticmethod
    def get(opts):

        for url in opts['seeds']:
            uid = uuid.uuid3(uuid.NAMESPACE_URL, url)
            with Content(opts['directory'], uid) as temp:
                temp.fetch(url, opts['depth'])
                objectname = uuid + '.' + opts['tag'] + '.warc.gz'
                result = temp.upload(opts['bucket'], objectname)
                yield {'seed': url, 'location': result}
