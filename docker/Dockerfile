#
# STEP 1 build executable binary
#

FROM golang:1.17.1-alpine as builder
#RUN apk add --no-cache git
#RUN apk add git

RUN adduser -D -g '' user
COPY ./ /tmp/sqspipe/
RUN rm -r /tmp/sqspipe/.git
RUN find /tmp/sqspipe
WORKDIR /tmp/sqspipe
RUN CGO_ENABLED=0 go build -o /bin/sqspipe ./sqspipe
RUN ls /bin/sqspipe

#
# STEP 2 build a small image from scratch
#
# start from scratch
# FROM scratch
# COPY --from=builder /etc/passwd /etc/passwd
# COPY --from=builder /bin/sqspipe /bin/sqspipe
# USER user
# ENTRYPOINT ["/bin/sqsuser"]

#
# STEP 2 build a small image from alpine
#
FROM alpine:3.14
COPY --from=builder /bin/sqspipe /bin/sqspipe
RUN adduser -D -g '' user
USER user
ENTRYPOINT ["/bin/sqspipe"]