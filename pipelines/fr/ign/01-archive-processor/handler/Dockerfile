ARG DOCKER_REPO=pwcasdnmp.azurecr.io
ARG NUCLIO_BASE_IMAGE=library/python-processor
ARG NUCLIO_BASE_IMAGE_TAG=alpine-3.6

# From the base image
FROM ${DOCKER_REPO}/${NUCLIO_BASE_IMAGE}:${NUCLIO_BASE_IMAGE_TAG}

ADD requirements.txt .

RUN apk --no-cache add \
        p7zip \
    	gcc \
    	g++ \
    	librdkafka-dev \ 
    	musl-dev && \
	apk --no-cache add \
		--repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
		--repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
		gdal-dev && \
    pip install -r requirements.txt && \
    apk del gcc g++ musl-dev

ADD main.py /opt/nuclio
