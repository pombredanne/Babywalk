# Babywalk

It's a web crawler based on [[GNU Wget][WgetHomepage]]. It reads its input
from [[Amazon S3][S3Homepage]] bucket and writes its output to the same
bucket. It's running inside a [[docker][DockerHomepage]] container.

[WgetHomepage]: http://www.gnu.org/software/wget/
[S3Homepage]: https://aws.amazon.com/s3/
[DockerHomepage]: https://www.docker.com/

## Input format

The input file shall have CSV like format, where:

* the first column represent some identifier,
* the second column is the depth of the crawling,
* the third and all the remaining columns are the seed URL.

An example could be:

    1234,1,http://example.com/

## Output format

The output will be a tar file, which content is:

* file `output` has the fetch and cleanup logs,
* file `seeds` has the seed URLs,
* directory `content` has the result of the crawl.

The `content` directory might be missing when wget could not get anything back.
(eg.: no DNS entry for the site.)

## How to build and run

To build the docker image with a tag `crawler` on it:

    $ docker build -t crawler .

To run the container with the same tag as before:

    $ docker run --env AWS_ACCESS_KEY_ID=<your_aws_access_key> \
                 --env AWS_SECRET_ACCESS_KEY=<your_aws_secret_key> \
                 -i -t crawler \
                 --bucket <s3_bucket_name> \
                 --seeds <file_name_on_s3_bucket>

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
