# Babywalk

It's a web crawler based on [GNU Wget][WgetHomepage]. It reads its input
from [RabbitMQ][RabbitmqHomepage] queue and writes its output to a given
[Amazon S3][S3Homepage] bucket. It's running inside a
[docker][DockerHomepage] container.

[WgetHomepage]: http://www.gnu.org/software/wget/
[RabbitmqHomepage]: http://www.rabbitmq.com/
[S3Homepage]: https://aws.amazon.com/s3/
[DockerHomepage]: https://www.docker.com/

It contains two programs: `crawl` which does the actual crawling and
`seed` which sends requests to the crawler.

## Input format

The input file shall have Tab-Separated-Values like format, where:

* the first column is the seed URL,
* the second column is some identifie.

An example could be:

    http://example.com/ 1234

## Output format

The output will be a gziped warc file. The filename is created from
the given identifier and with extension '.warc.gz'.

## How to build and run

To build the docker image with a tag `crawler` on it:

    $ docker build -t crawler .

To run the container with the same tag as before:

    $ docker run --env AWS_ACCESS_KEY_ID=<your_aws_access_key> \
                 --env AWS_SECRET_ACCESS_KEY=<your_aws_secret_key> \
                 -v <directory_from_host>:/data \
                 -i -t crawler

## Improvement ideas

Content filtering is done after fetch. Because `wget` does not support
filtering MIME type. A better way could be to set up a proxy which does
the filtering and return `403 Forbidden` when the type shouldn't be accepted.

To improve the idea of proxy filtering. The proxy can send `INFO` request
instead of `GET` first. And resend the `GET` only if the content type is
okay.

## Problem reports

Although there are ideas to improve it. This project was a one shot from my
side. It was rather interesting to see how to solve the basic problems.
I don't really need the functionality anymore... But if somebody find it
useful and found problems and bugs, would be more than glad to hear about those.
