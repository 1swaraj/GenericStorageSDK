FROM gcr.io/google.com/cloudsdktool/cloud-sdk:alpine as build
# install build tools
RUN apk add go git
# cache dependencies
ADD go.mod go.sum ./
RUN go mod download
# build
ADD . .
RUN go build -o /main
# copy artifacts to a clean image
FROM alpine
COPY --from=build /main /main
ENTRYPOINT [ "/main" ]