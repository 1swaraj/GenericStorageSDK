FROM gcr.io/google.com/cloudsdktool/cloud-sdk:alpine as build
RUN \
    apk -Uuv add groff less python3 py-pip && \
    pip install awscli && \
    rm /var/cache/apk/*
COPY /config /config
COPY /credentials /credentials
COPY /key.json /key.json
RUN mkdir ~/.aws
RUN cp /config ~/.aws/config
RUN cp /credentials ~/.aws/credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/key.json"
# install build tools
RUN apk add go git
# cache dependencies
ADD go.mod go.sum ./
RUN go mod download
# build
ADD . .
RUN go build -o /main
WORKDIR /
ENTRYPOINT [ "/main" ]