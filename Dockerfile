FROM base/archlinux

RUN pacman -Sy --noconfirm wget python python-pip

ADD . /opt/crawler
RUN pip install -r /opt/crawler/requirements.txt
RUN pip install /opt/crawler

VOLUME /data

ENV AWS_ACCESS_KEY_ID=changeme AWS_SECRET_ACCESS_KEY=changeme

ENTRYPOINT ["/usr/sbin/crawl", "--buffer", "/data"]
