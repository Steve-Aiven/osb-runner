FROM opensearchproject/opensearch-benchmark:2.3.0

USER root

# Pre-seed the geonames corpus during the image build so the container does
# not need to download it (253 MB) at runtime on every redeploy.
COPY seed_corpus.py /tmp/seed_corpus.py
RUN set -eux; \
    opensearch-benchmark list workloads; \
    python3 /tmp/seed_corpus.py; \
    rm /tmp/seed_corpus.py

WORKDIR /app
COPY run.py .

EXPOSE 8080

CMD ["python3", "run.py"]
