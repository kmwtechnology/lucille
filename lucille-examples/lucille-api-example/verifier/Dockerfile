FROM alpine:3.20
RUN apk add --no-cache curl jq

COPY pipeline.conf /pipeline.conf
COPY verify.sh /verify.sh
RUN chmod +x /verify.sh

ENTRYPOINT ["/verify.sh"]
