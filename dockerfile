###DEPRECIATED DO NOT USE IT
RUN exit 1
FROM python:3.11-alpine AS builder

RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    python3-dev \
    zsh

WORKDIR /cworkers

COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

COPY . .

#########
FROM python:3.11-alpine

WORKDIR /cworkers
RUN apk add --no-cache zsh

COPY --from=builder /install /usr/local
COPY --from=builder /cworkers /cworkers

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
#No more needed due to the change in the design, may be used in the future tho.
#ENTRYPOINT ["/entrypoint.sh"]
CMD ["python3", "-u", "setup.py"]
